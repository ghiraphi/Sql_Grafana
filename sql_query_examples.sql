'''
пример SQL-запроса PostgreSQL для статистики работы пользователей со словарями
показывает количество внесённых за день новых строк
используется для визуализации в Grafana 
значение User1 меняется на имя в бд, добавляются другие имена
'''

with tabla as (
SELECT  
to_timestamp(creation_timestamp), 
(creator->'full_name') as imya, 
dictionary_id, 
LENGTH(REPLACE(log_text, ':cs', ':cs*'))-LENGTH(log_text) as dlina,
abs((LENGTH(REPLACE(log_text, ':cs', ':cs*'))-LENGTH(log_text))   - (LEAD(  LENGTH(REPLACE(log_text, ':cs', ':cs*'))-LENGTH(log_text)) OVER (PARTITION BY dictionary_id order by creation_timestamp desc)   ) )  as raznica
,log_text
,lead(log_text) over (PARTITION BY dictionary_id order by creation_timestamp desc)
FROM dic_log
where (creator->'full_name')::text ~ '.*User1.*' and to_timestamp(creation_timestamp)>=current_date
ORDER BY creation_timestamp DESC 
)
select sum(raznica) from tabla

'''
пример SQL-запроса PostgreSQL для отображения точности работы словаря по заданной теме за период
показывает точность словаря за каждый день
используется для визуализации в Grafana 
значение Name_topic меняется на id темы в бд, добавляются другие темы
'''

SELECT 
   d as "Период",
   CAST(COUNT(CASE WHEN status='no_inf' THEN 0 END) AS FLOAT) /   COUNT(*) as "Точность - название_темы" 
FROM 
  generate_series('2021-01-01', NOW(), interval '1 day') as gs(d) 
  left join sur s 
  ON date_trunc('hour', to_timestamp(s.created_at)) = gs.d
where
array_position(an_top, 'Name_topic') IS NOT NULL and
is_manual = 'False'
GROUP BY 1 ORDER BY 1;
