-- Rows are kept forever: a NAB replay spans a month, and the 24-hour TTL dropped all but the last day.
ALTER TABLE metrics REMOVE TTL;
ALTER TABLE evaluations REMOVE TTL;
