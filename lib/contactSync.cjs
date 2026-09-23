const SOURCE_FIELDS = [
  'firstname',
  'lastname',
  'email',
  'phone',
  'owner_id',
  'lead_status',
  'lifecycle_stage',
];

const DATE_FIELDS = ['hubspot_created_date', 'owner_assigned_date'];

function normalizedDate(value) {
  if (value == null || value === '') return null;
  const dateValue = typeof value === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?$/.test(value)
    ? `${value}Z`
    : value;
  const timestamp = new Date(dateValue).getTime();
  return Number.isFinite(timestamp) ? timestamp : String(value);
}

function contactSourceChanged(existing, incoming) {
  if (!existing) return true;
  if (SOURCE_FIELDS.some((field) => (existing[field] ?? '') !== (incoming[field] ?? ''))) {
    return true;
  }
  return DATE_FIELDS.some((field) => normalizedDate(existing[field]) !== normalizedDate(incoming[field]));
}

async function loadExistingContacts(supabase, contacts) {
  const existing = new Map();
  const ids = [...new Set(contacts.map((contact) => String(contact.id)))];
  const columns = ['hubspot_id', ...SOURCE_FIELDS, ...DATE_FIELDS].join(',');

  for (let offset = 0; offset < ids.length; offset += 100) {
    const { data, error } = await supabase
      .from('contacts')
      .select(columns)
      .in('hubspot_id', ids.slice(offset, offset + 100));
    if (error) throw new Error(`Contact comparison read failed: ${error.code || 'database_error'}`);
    for (const row of data || []) existing.set(String(row.hubspot_id), row);
  }

  return existing;
}

module.exports = { contactSourceChanged, loadExistingContacts };
