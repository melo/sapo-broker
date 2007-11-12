CREATE TABLE IF NOT EXISTS Message (destination varchar(100), priority int, mtimestamp bigint, sequence_nr bigint, msg_id varchar(50), delivery_count int, msg nvarchar);
CREATE INDEX IF NOT EXISTS idx_01 ON Message(destination);
CREATE INDEX IF NOT EXISTS idx_02 ON Message(msg_id);