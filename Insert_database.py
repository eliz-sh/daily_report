
# coding: utf-8

# In[5]:


import postgresql
import numpy as np
# import postgresql.driver as pg_driver


# In[6]:


#DB streetbee
db1 = pg_driver.connect(user='streetbee_analytics', password='ecmCZZNvLwAHk7onTVXC',host='pg-001-analysis.cnysugxwnx0x.eu-central-1.rds.amazonaws.com', database='streetbee', port=5432)

#Local DB
db2 = pg_driver.connect(user='elizaveta_sb', host='localhost', database='streetbee_analytics', port=5432)


# # Insert for 1 Sheet

# In[7]:


check=db2.query("DELETE FROM report_sheet1 WHERE date=current_date")

current_completed_1 = db2.prepare("INSERT INTO report_sheet1  VALUES ($1, $2, now())")

data=db1.query("WITH active_projects AS (SELECT id FROM task_projects WHERE published_at <= now() AND deadline_at > now() - interval '1 week') SELECT id, receive FROM (SELECT published_tasks.task_project_id, count(*) AS receive FROM published_tasks JOIN reserved_tasks ON published_tasks.id = reserved_tasks.published_task_id WHERE reserved_tasks.state = 'response_accepted' OR reserved_tasks.state = 'response_received' GROUP BY task_project_id) AS t1 JOIN active_projects ON t1.task_project_id = active_projects.id ORDER BY id;")

for row in data:
    current_completed_1(row[0], row[1])


# # Insert for 2 Sheet

# In[8]:


check=db2.query("DELETE FROM report_sheet2 WHERE date=current_date")
current_completed_2 = db2.prepare("INSERT INTO report_sheet2 VALUES ($1, $2, $3, $4, now() ) ")
data=db1.query("""
WITH active_projects AS (SELECT  DISTINCT wbs_code FROM task_projects
WHERE published_at <= now() AND deadline_at > now() AND (task_projects.id != 573))
SELECT task_points.region, task_points.city, task_projects.wbs_code, Count(*) AS Completed
FROM reserved_tasks INNER JOIN published_tasks ON published_tasks.id = reserved_tasks.published_task_id
INNER JOIN task_points ON task_points.id = published_tasks.task_point_id
INNER JOIN task_projects ON task_projects.id = published_tasks.task_project_id
WHERE task_projects.wbs_code IN (SELECT wbs_code FROM active_projects)
AND (reserved_tasks.state = 'response_received' OR reserved_tasks.state = 'response_accepted')
AND published_tasks.type = 'PublishedTasks::PointTask'
GROUP BY task_points.region, task_projects.wbs_code, task_points.city ORDER BY task_points.region, city;""")

for row in data:
    current_completed_2(row[0], row[1], row[2], row[3])


# In[ ]:
