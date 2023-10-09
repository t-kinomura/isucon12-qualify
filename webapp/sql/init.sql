DELETE FROM tenant WHERE id > 100;
DELETE FROM visit_history WHERE created_at >= '1654041600';
CREATE INDEX visit_history_multi_index ON visit_history (tenant_id, competition_id, player_id);
-- UPDATE id_generator SET id=2678400000 WHERE stub='a';
-- ALTER TABLE id_generator AUTO_INCREMENT=2678400000;
