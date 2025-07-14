import uuid
import json
from src.services.door_service import fetch_door_data_by_id, insert_door_data_to_destination, get_door_from_destination
from src.services.employee_service import fetch_employee_data_by_id, insert_employee_data_to_destination, get_employee_from_destination
from src.services.media_service import fetch_alarm_media_by_alarm_id, insert_alarm_media_and_get_id
from src.services.ml_service import fetch_ml_output_by_id, fetch_video_tags_by_ml_output_id, insert_ml_output_to_destination, insert_video_tags_to_destination
from src.services.alarm_type_service import get_alarm_type_id, get_alarm_type, get_respective_alarm_type_id_from_destination
from src.services.alarm_update_service import fetch_alarm_updates, insert_alarm_updates

def prepare_new_raw_alarm_data(
    original_alarm_data,
    new_camera_id,
    new_tenant_id,
    alarm_type_id,
    new_door_id,
    new_employee_id,
    new_user_id=None,
    new_latest_media_id=None
):
    new_alarm = original_alarm_data.copy()
    new_alarm['id'] = str(uuid.uuid4())
    new_alarm['source_entity_id'] = new_camera_id
    new_alarm['tenant_id'] = new_tenant_id
    new_alarm['latest_alarm_media_id'] = new_latest_media_id
    new_alarm['door_id'] = new_door_id
    new_alarm['employee_id'] = new_employee_id
    new_alarm['user_id'] = new_user_id
    if alarm_type_id:
        new_alarm['alarm_type_id'] = alarm_type_id
    else:
        print("⚠️ Warning: Alarm type ID is None. Assigning default motion-detected type.")
        new_alarm['alarm_type_id'] = "acf15f45-1c8f-426a-8b78-5dbbfef95c36"
    for key, value in new_alarm.items():
        if isinstance(value, (dict, list)):
            new_alarm[key] = json.dumps(value)
    return new_alarm

def insert_raw_alarm(connection, alarm_data):
    cursor = connection.cursor()
    columns = ', '.join(alarm_data.keys())
    placeholders = ', '.join(['%s'] * len(alarm_data))
    query = f"INSERT INTO raw_alarms_v2 ({columns}) VALUES ({placeholders})"
    cursor.execute(query, list(alarm_data.values()))
    connection.commit()
    return True 