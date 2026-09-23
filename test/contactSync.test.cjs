const { test } = require('node:test');
const assert = require('node:assert/strict');

const { contactSourceChanged, loadExistingContacts } = require('../lib/contactSync.cjs');

const source = {
  firstname: 'A', lastname: 'B', email: 'a@example.test', phone: '123',
  owner_id: 'owner', lead_status: 'OPEN', lifecycle_stage: 'lead',
  hubspot_created_date: new Date('2026-09-01T12:00:00Z'),
  owner_assigned_date: null,
};

test('unchanged contact ignores last_synced_at and sync state', () => {
  const existing = {
    ...source,
    hubspot_created_date: '2026-09-01T12:00:00',
    last_synced_at: '2026-09-01T12:01:00Z',
    synced_to_hubspot: true,
  };
  assert.equal(contactSourceChanged(existing, source), false);
  assert.equal(contactSourceChanged(null, source), true);
});

test('changed source fields and dates require an upsert', () => {
  assert.equal(contactSourceChanged({ ...source, phone: '456' }, source), true);
  assert.equal(contactSourceChanged({ ...source, owner_assigned_date: '2026-09-02T00:00:00Z' }, source), true);
});

test('comparison batches reads and fails closed on a database error', async () => {
  const batches = [];
  const supabase = {
    from(table) {
      assert.equal(table, 'contacts');
      return {
        select() {
          return {
            async in(field, ids) {
              assert.equal(field, 'hubspot_id');
              batches.push(ids);
              return { data: ids.map((hubspot_id) => ({ hubspot_id })) };
            },
          };
        },
      };
    },
  };
  const contacts = Array.from({ length: 201 }, (_, index) => ({ id: index + 1 }));
  const existing = await loadExistingContacts(supabase, contacts);
  assert.deepEqual(batches.map((batch) => batch.length), [100, 100, 1]);
  assert.equal(existing.size, 201);

  const failed = {
    from() {
      return { select() { return { async in() { return { error: { code: 'timeout' } }; } }; } };
    },
  };
  await assert.rejects(loadExistingContacts(failed, [{ id: 1 }]), /comparison read failed/);
});
