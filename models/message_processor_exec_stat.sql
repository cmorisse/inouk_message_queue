SELECT
    --message_id,
    imp.processor_id,
    impr.name,
    COUNT(imp.id) AS nb_messages,
    MIN(imp.processing_time),
    AVG(imp.processing_time),
    MAX(imp.processing_time),
    ROUND(MAX(imp.processing_time) * 1.5) AS recommended_timeout_s

FROM imq_message_processing AS imp
    LEFT JOIN imq_message_processor AS impr ON imp.processor_id = impr.id
GROUP BY imp.state, imp.processor_id, impr.name
HAVING state='done';