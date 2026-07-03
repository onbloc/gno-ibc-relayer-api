-- tx_out/tx_in replace tx_hash and are appended at the end of the table
-- (rather than an in-place rename) so they sit after all existing columns.
ALTER TABLE transfers ADD COLUMN IF NOT EXISTS tx_out TEXT;
UPDATE transfers SET tx_out = tx_hash WHERE tx_out IS NULL;
ALTER TABLE transfers DROP COLUMN IF EXISTS tx_hash;
ALTER TABLE transfers ADD COLUMN IF NOT EXISTS tx_in TEXT;
