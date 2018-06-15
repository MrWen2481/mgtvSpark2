SELECT ifnull(count(1), 0) AS pv  
                
FROM owlx.mid_pageview_day  
                
WHERE offset_id IS NOT NULL 
                 AND dt = '20180530'
                 AND platform = 'HNDX'