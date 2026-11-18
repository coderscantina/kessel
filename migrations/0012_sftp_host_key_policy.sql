-- The bookmark form no longer offers "accept" (ssh StrictHostKeyChecking=no,
-- which also accepts changed host keys). Sites that picked it keep
-- connecting without a prompt via trust-on-first-use ("add"), but a changed
-- key now blocks the connection like it should.
UPDATE sites
   SET options = json_set(options, '$.known_hosts_strategy', 'add')
 WHERE scheme = 'sftp'
   AND json_extract(options, '$.known_hosts_strategy') = 'accept';
