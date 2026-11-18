-- A task isn't claimed while another one writes the same destination. The
-- expression is the normalized path `store::claim_sql` compares.
CREATE INDEX idx_transfer_tasks_active_dst
    ON transfer_tasks(ltrim(replace(dst, '//', '/'), '/')) WHERE state = 'active';
