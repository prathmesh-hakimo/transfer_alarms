from src.utils.logger import get_logger
logger = get_logger("alarm_type_service")


def get_alarm_type_id(source_connection, raw_alarm_id):
    cursor = source_connection.cursor(dictionary=True)
    cursor.execute("SELECT alarm_type_id FROM raw_alarms_v2 WHERE id = %s", (raw_alarm_id,))
    result = cursor.fetchone()
    if not result:
        logger.warning(f"No alarm found with ID {raw_alarm_id}, using predefined value = acf15f45-1c8f-426a-8b78-5dbbfef95c36")
        return "acf15f45-1c8f-426a-8b78-5dbbfef95c36"
    return result['alarm_type_id']


def get_alarm_type(source_connection, alarm_type_id):
    cursor = source_connection.cursor(dictionary=True)
    cursor.execute("SELECT alarm_type FROM alarm_types WHERE id = %s", (alarm_type_id,))
    result = cursor.fetchone()
    if not result:
        logger.warning(f"No alarm type found with ID {alarm_type_id}, using predefined one (ALARM TYPE: Motion Detected).")
        return "Motion Detected"
    return result['alarm_type']


def get_respective_alarm_type_id_from_destination(destination_connection, alarm_type):
    cursor = destination_connection.cursor(dictionary=True)
    cursor.execute("SELECT id FROM alarm_types WHERE alarm_type = %s", (alarm_type,))
    results = cursor.fetchall()
    if not results:
        logger.warning(f"No alarm type found for type '{alarm_type}'")
        return None
    if len(results) > 1:
        logger.warning(f"Multiple alarm types found for type '{alarm_type}', using the first one.")
    return results[0]['id'] 