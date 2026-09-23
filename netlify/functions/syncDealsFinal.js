const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const { contactSourceChanged, loadExistingContacts } = require('../../lib/contactSync.cjs');

dayjs.extend(utc);
dayjs.extend(timezone);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

exports.handler = async (event) => {
  try {
    // Add execution protection to prevent overlapping runs
    const { data: runningSync, error: runningError } = await supabase
      .from('b_sync_logs')
      .select('created_at')
      .eq('function_name', 'syncDealsFinal')
      .eq('status', 'running')
      .gte('created_at', dayjs().subtract(5, 'minutes').toISOString())
      .maybeSingle();
    if (runningError) {
      console.error('Contact sync guard unavailable:', runningError.code || 'database_error');
      return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: 'database_unavailable' }) };
    }

    const { data: recentPause, error: pauseError } = await supabase
      .from('b_sync_logs')
      .select('created_at')
      .eq('function_name', 'syncDealsFinal')
      .eq('status', 'paused')
      .gte('created_at', dayjs().subtract(10, 'minutes').toISOString())
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (pauseError) {
      console.error('Contact sync pause check unavailable:', pauseError.code || 'database_error');
      return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: 'database_unavailable' }) };
    }
    if (recentPause) {
      return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: 'database_pressure' }) };
    }
    
    if (runningSync) {
      console.log('⏸️ Sync already running, skipping this execution');
      return { statusCode: 200, body: JSON.stringify({ message: 'Sync already running' }) };
    }
    
    // Mark this sync as running
    const { error: startLogError } = await supabase.from('b_sync_logs').insert({
      id: require('crypto').randomUUID(),
      function_name: 'syncDealsFinal',
      status: 'running',
      message: 'Sync started',
      created_at: new Date().toISOString()
    });
    if (startLogError) {
      console.error('Contact sync start log unavailable:', startLogError.code || 'database_error');
      return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: 'database_unavailable' }) };
    }
    // --- THIS IS THE LINE THAT WAS CHANGED ---
    const isoStart = dayjs().tz('America/Chicago').startOf('month').toISOString();
    
    // For initial load: fetch 60 days of contacts
    // For ongoing sync: only fetch contacts modified in last 24 hours
    
    // Check contact count to determine if we need full resync
    const { count: contactCount, error: countError } = await supabase
      .from('b_contacts')
      .select('hubspot_id', { count: 'exact', head: true });
    if (countError) throw new Error(`Contact count failed: ${countError.code || 'database_error'}`);
    
    // Force initial load if we have suspiciously few contacts (like 300)
    const forceInitialLoad = contactCount < 1000;
    
    const { data: lastSync, error: lastSyncError } = await supabase
      .from('b_sync_logs')
      .select('created_at')
      .eq('function_name', 'syncDealsFinal')
      .eq('status', 'success')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (lastSyncError) throw new Error(`Last sync lookup failed: ${lastSyncError.code || 'database_error'}`);
    
    const isInitialLoad = !lastSync || forceInitialLoad;
    const contactsStartDate = isInitialLoad 
      ? dayjs().tz('America/Chicago').subtract(60, 'days').toISOString()
      : dayjs().tz('America/Chicago').subtract(24, 'hours').toISOString();
    
    console.log(`📋 Contact sync mode: ${isInitialLoad ? 'INITIAL (60 days)' : 'INCREMENTAL (24 hours)'} ${forceInitialLoad ? '[FORCED]' : ''}`);
    
    const now = new Date().toISOString();

    const HUBSPOT_HEADERS = {
      Authorization: `Bearer ${process.env.HUBSPOT_PRIVATE_APP_TOKEN}`,
      'Content-Type': 'application/json'
    };
    
    let allDeals = [];
    let allContacts = [];
    let dealAfter = null;
    let contactAfter = null;
    let hasMoreDeals = false;
    let hasMoreContacts = false;
    let totalDealsFetched = 0;
    let totalContactsFetched = 0;

    console.log(`🚀 Starting HubSpot sync for deals and contacts...`);

    // Helper function to add delay between API calls
    const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    // DEALS SYNC
    do {
      const dealsResponse = await axios.post(
        'https://api.hubapi.com/crm/v3/objects/deals/search',
        {
          filterGroups: [
            {
              filters: [
                {
                  propertyName: 'closedate',
                  operator: 'GTE',
                  value: isoStart
                },
                {
                  propertyName: 'hs_deal_stage_probability',
                  operator: 'EQ',
                  value: 1 
                }
              ]
            }
          ],
          properties: ['dealname', 'amount', 'pipeline', 'dealstage', 'closedate', 'hubspot_owner_id'],
          limit: 100,
          after: dealAfter
        },
        { headers: HUBSPOT_HEADERS }
      );

      const dealsOnPage = dealsResponse.data.results || [];
      allDeals = allDeals.concat(dealsOnPage);
      totalDealsFetched += dealsOnPage.length;

      if (dealsResponse.data.paging && dealsResponse.data.paging.next) {
        hasMoreDeals = true;
        dealAfter = dealsResponse.data.paging.next.after;
      } else {
        hasMoreDeals = false;
      }

      // Add delay to avoid rate limits
      if (hasMoreDeals) {
        await delay(200); // 200ms delay between paginated deal requests
      }

    } while (hasMoreDeals);

    console.log(`✅ Fetched ${totalDealsFetched} deals from HubSpot.`);

    // Add delay before starting contacts sync to avoid rate limits
    await delay(1000);

    // CONTACTS SYNC - Use date windowing to bypass 300 result API limit
    console.log(`📅 Starting contact sync with ${isInitialLoad ? '7-day' : '24-hour'} windows...`);
    
    if (isInitialLoad) {
      // For initial load: fetch in 3-day windows to avoid 30s timeout
      const startDate = dayjs(contactsStartDate).tz('America/Chicago');
      const endDate = dayjs().tz('America/Chicago');
      let currentWindowStart = startDate;
      let windowCount = 0;
      const maxWindows = 5; // Process max 5 windows per execution (15 days)
      
      // Check if we have contacts from recent dates to continue from where we left off
      const { data: lastContact } = await supabase
        .from('b_contacts')
        .select('hubspot_created_date')
        .order('hubspot_created_date', { ascending: false })
        .limit(1)
        .single();
      
      if (lastContact?.hubspot_created_date) {
        const lastContactDate = dayjs(lastContact.hubspot_created_date).tz('America/Chicago');
        if (lastContactDate.isAfter(startDate)) {
          currentWindowStart = lastContactDate.add(1, 'day');
          console.log(`📍 Resuming from ${currentWindowStart.format('YYYY-MM-DD')} (last contact: ${lastContactDate.format('YYYY-MM-DD')})`);
        }
      }
      
      console.log(`📊 Will fetch contacts from ${startDate.format('YYYY-MM-DD')} to ${endDate.format('YYYY-MM-DD')}`);
      
      while (currentWindowStart.isBefore(endDate) && windowCount < maxWindows) {
        windowCount++;
        const windowEnd = currentWindowStart.add(3, 'days'); // 3-day windows for faster processing
        const windowEndCapped = windowEnd.isAfter(endDate) ? endDate : windowEnd;
        
        console.log(`📋 Fetching contacts: ${currentWindowStart.format('YYYY-MM-DD')} to ${windowEndCapped.format('YYYY-MM-DD')}`);
        
        // Fetch contacts for this 3-day window
        contactAfter = null;
        hasMoreContacts = false;
        
        do {
          const contactsResponse = await axios.post(
            'https://api.hubapi.com/crm/v3/objects/contacts/search',
            {
              filterGroups: [
                {
                  filters: [
                    {
                      propertyName: 'createdate',
                      operator: 'GTE',
                      value: currentWindowStart.toISOString()
                    },
                    {
                      propertyName: 'createdate',
                      operator: 'LTE',
                      value: windowEndCapped.toISOString()
                    },
                    {
                      propertyName: 'invalid_lead',
                      operator: 'NOT_HAS_PROPERTY'
                    }
                  ]
                }
              ],
              properties: [
                'firstname', 'lastname', 'email', 'phone', 'hubspot_owner_id',
                'createdate', 'hs_lead_status', 'lifecyclestage', 'hs_analytics_source',
                'hubspot_owner_assigneddate'
              ],
              limit: 100,
              after: contactAfter
            },
            { headers: HUBSPOT_HEADERS }
          );

          const contactsOnPage = contactsResponse.data.results || [];
          allContacts = allContacts.concat(contactsOnPage);
          totalContactsFetched += contactsOnPage.length;

          if (contactsResponse.data.paging && contactsResponse.data.paging.next) {
            hasMoreContacts = true;
            contactAfter = contactsResponse.data.paging.next.after;
          } else {
            hasMoreContacts = false;
          }

          // Reduce delays to fit within 30s timeout
          if (hasMoreContacts) {
            await delay(100); // Faster pagination
          }

        } while (hasMoreContacts);
        
        // Move to next 3-day window
        currentWindowStart = windowEnd;
        
        // Reduce delay between windows
        await delay(200);
        
        console.log(`📈 Window ${windowCount}/${maxWindows} complete. Total contacts so far: ${totalContactsFetched}`);
      }
      
      console.log(`✅ Completed ${windowCount} windows. Fetched ${totalContactsFetched} contacts in initial load.`);
      
    } else {
      // For incremental sync: simple 24-hour window (should be <300 results)
      do {
        const contactsResponse = await axios.post(
          'https://api.hubapi.com/crm/v3/objects/contacts/search',
          {
            filterGroups: [
              {
                filters: [
                  {
                    propertyName: 'lastmodifieddate',
                    operator: 'GTE',
                    value: contactsStartDate
                  },
                  {
                    propertyName: 'invalid_lead',
                    operator: 'NOT_HAS_PROPERTY'
                  }
                ]
              }
            ],
            properties: [
              'firstname', 'lastname', 'email', 'phone', 'hubspot_owner_id',
              'createdate', 'hs_lead_status', 'lifecyclestage', 'hs_analytics_source',
              'hubspot_owner_assigneddate'
            ],
            limit: 100,
            after: contactAfter
          },
          { headers: HUBSPOT_HEADERS }
        );

        const contactsOnPage = contactsResponse.data.results || [];
        allContacts = allContacts.concat(contactsOnPage);
        totalContactsFetched += contactsOnPage.length;

        if (contactsResponse.data.paging && contactsResponse.data.paging.next) {
          hasMoreContacts = true;
          contactAfter = contactsResponse.data.paging.next.after;
        } else {
          hasMoreContacts = false;
        }

        // Add delay between contact pagination requests
        if (hasMoreContacts) {
          await delay(300);
        }

      } while (hasMoreContacts);
    }

    console.log(`✅ Fetched ${totalContactsFetched} contacts from HubSpot using windowed approach.`);

    // SYNC DEALS TO SUPABASE
    let dealsUpserted = 0;

    for (const deal of allDeals) {
      const { id, properties } = deal;

      const dealRecord = {
        hubspot_id: id,
        hubspot_owner_id: properties.hubspot_owner_id || null,
        dealname: properties.dealname || '',
        amount: parseFloat(properties.amount || 0),
        pipeline: properties.pipeline || '',
        dealstage: properties.dealstage || '',
        closedate: properties.closedate ? new Date(properties.closedate) : null,
        synced_to_hubspot: false,
        last_synced_at: now
      };

      const { error } = await supabase
        .from('deals')
        .upsert(dealRecord, { onConflict: 'hubspot_id' });

      if (!error) {
        dealsUpserted++;
      } else {
        console.error(`❌ Deal upsert error for ${id}:`, error.message);
      }
    }

    // SYNC CONTACTS TO SUPABASE
    let contactsUpserted = 0;
    let contactsUnchanged = 0;
    let contactsFailed = 0;
    const uniqueContacts = [...new Map(allContacts.map((contact) => [String(contact.id), contact])).values()];
    const existingContacts = await loadExistingContacts(supabase, uniqueContacts);
    for (const contact of uniqueContacts) {
      const { id, properties } = contact;

      const contactRecord = {
        hubspot_id: id,
        firstname: properties.firstname || '',
        lastname: properties.lastname || '',
        email: properties.email || '',
        phone: properties.phone || '',
        owner_id: properties.hubspot_owner_id || null,
        hubspot_created_date: properties.createdate ? new Date(properties.createdate) : null,
        owner_assigned_date: properties.hubspot_owner_assigneddate ? new Date(properties.hubspot_owner_assigneddate) : null,
        lead_status: properties.hs_lead_status || '',
        lifecycle_stage: properties.lifecyclestage || '',
        synced_to_hubspot: false,
        last_synced_at: now
      };

      if (!contactSourceChanged(existingContacts.get(String(id)), contactRecord)) {
        contactsUnchanged++;
        continue;
      }

      const { error } = await supabase
        .from('contacts')
        .upsert(contactRecord, { onConflict: 'hubspot_id' });

      if (!error) {
        contactsUpserted++;
      } else {
        contactsFailed++;
        console.error(`❌ Contact upsert error for ${id}:`, error.message);
      }
    }

    // Mark running sync as complete and log success
    await supabase.from('b_sync_logs')
      .update({ status: 'completed' })
      .eq('function_name', 'syncDealsFinal')
      .eq('status', 'running');
      
    const successMessage = `Synced ${dealsUpserted}/${totalDealsFetched} deals; contacts: ${contactsUpserted} written, ${contactsUnchanged} unchanged, ${contactsFailed} failed.`;
    
    await supabase.from('b_sync_logs').insert({
      id: require('crypto').randomUUID(),
      function_name: 'syncDealsFinal',
      status: 'success',
      message: successMessage,
      created_at: now
    });

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: successMessage,
        timestamp: now,
        deals_synced: dealsUpserted,
        contacts_synced: contactsUpserted,
        contacts_unchanged: contactsUnchanged,
        contacts_failed: contactsFailed,
        sync_mode: isInitialLoad ? 'initial' : 'incremental'
      })
    };
  } catch (err) {
    // Mark running sync as failed
    await supabase.from('b_sync_logs')
      .update({ status: 'error', message: err.message })
      .eq('function_name', 'syncDealsFinal')
      .eq('status', 'running');
      
    if (err.response) {
      console.error('HubSpot API Error:', err.response.data);
    }
    console.error('Full error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: `❌ Sync failed: ${err.message}` })
    };
  }
};
