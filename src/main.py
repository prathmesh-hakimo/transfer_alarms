import argparse
from src.database.config import source, destination
from src.database.connection import connect_to_database
from src.services.door_service import fetch_door_data_by_id, insert_door_data_to_destination, get_door_from_destination
from src.services.employee_service import fetch_employee_data_by_id, insert_employee_data_to_destination, get_employee_from_destination
from src.services.media_service import fetch_alarm_media_by_alarm_id, insert_alarm_media_and_get_id
from src.services.ml_service import fetch_ml_output_by_id, fetch_video_tags_by_ml_output_id, insert_ml_output_to_destination, insert_video_tags_to_destination
from src.services.alarm_type_service import get_alarm_type_id, get_alarm_type, get_respective_alarm_type_id_from_destination
from src.services.alarm_update_service import fetch_alarm_updates, insert_alarm_updates
from src.services.alarm_service import prepare_new_raw_alarm_data, insert_raw_alarm
from src.models.state import DuplicationState
from src.utils.logger import get_logger, add_file_handler
import uuid
import json

def main(original_raw_alarm_id):
    new_camera_id = '259e78d5-6ed1-4853-8b50-ca5413d0e2b4'
    new_tenant_id = 'demo-sales'

    logger = get_logger("alarm_migration")
    add_file_handler(logger, original_raw_alarm_id)
    state = DuplicationState()

    logger.info("🚀 Starting Alarm Migration Process")

    source_conn = connect_to_database(source)
    dest_conn = connect_to_database(destination)

    if not source_conn or not dest_conn:
        logger.error("❌ Failed to connect to source or destination DB. Exiting.")
        return

    # 1️⃣ Fetch original raw alarm
    original_alarm = fetch_raw_alarm_by_id(source_conn, original_raw_alarm_id)
    if not original_alarm:
        logger.error("❌ Raw alarm not found. Exiting.")
        return
    logger.info("✅ Original alarm fetched.")

    # Extract unique fields for duplicate check
    source_id = original_alarm.get('source_id')
    partition_key = original_alarm.get('partition_key')
    tenant_id = new_tenant_id

    # Check for duplicate in destination
    from src.services.alarm_service import find_existing_raw_alarm, find_conflicting_location_alarm_info
    existing_alarm = find_existing_raw_alarm(dest_conn, source_id, tenant_id, partition_key)
    if existing_alarm:
        logger.warning(
            f"⚠️ Duplicate alarm detected in destination for source_id={source_id}, tenant_id={tenant_id}, partition_key={partition_key}. Skipping migration."
        )
        conflict_info = find_conflicting_location_alarm_info(dest_conn, source_id, tenant_id, partition_key)
        if conflict_info:
            raw_alarm = conflict_info.get('raw_alarm')
            camera = conflict_info.get('camera')
            location_alarms = conflict_info.get('location_alarms')
            # Log only important fields
            if raw_alarm:
                important_fields = ['id', 'source_id', 'partition_key', 'alarm_type_id', 'source_entity_id', 'tenant_id', 'alarm_timestamp_utc', 'current_status']
                logger.warning("Existing Raw Alarm Details:")
                for field in important_fields:
                    logger.warning(f"  {field}: {raw_alarm.get(field)}")
            else:
                logger.warning("Raw Alarm: Not found")
            if camera:
                logger.warning(f"Camera ID: {camera.get('id')}, Name: {camera.get('camera_name')}, Location ID: {camera.get('location_id')}")
            else:
                logger.warning("Camera: Not found")
            if location_alarms:
                logger.warning("Location Alarm IDs with same location:")
                for la in location_alarms:
                    logger.warning(f"  {la.get('id')}")
            else:
                logger.warning("No location alarms found for this location.")
        return

    # 2️⃣ Handle Door
    new_door_id = None
    door_id = original_alarm.get('door_id')
    if door_id:
        door_data = fetch_door_data_by_id(source_conn, door_id)
        if door_data:
            existing_door = get_door_from_destination(dest_conn, door_data)
            if existing_door:
                new_door_id = existing_door['id']
                logger.info(f"Door already exists in destination with ID: {new_door_id}")
            else:
                new_door_id = str(uuid.uuid4())
                insert_door_data_to_destination(dest_conn, door_data, new_door_id)
                logger.info(f"Inserted new door with ID: {new_door_id}")

    # 3️⃣ Handle Employee
    new_employee_id = None
    employee_id = original_alarm.get('employee_id')
    if employee_id:
        employee_data = fetch_employee_data_by_id(source_conn, employee_id)
        if employee_data:
            existing_employee = get_employee_from_destination(dest_conn, employee_data)
            if existing_employee:
                new_employee_id = existing_employee['id']
                logger.info(f"Employee already exists in destination with ID: {new_employee_id}")
            else:
                new_employee_id = str(uuid.uuid4())
                insert_employee_data_to_destination(dest_conn, employee_data, new_employee_id)
                logger.info(f"Inserted new employee with ID: {new_employee_id}")

    # 4️⃣ Resolve Alarm Type
    source_alarm_type_id = get_alarm_type_id(source_conn, original_raw_alarm_id)
    source_alarm_type = get_alarm_type(source_conn, source_alarm_type_id)
    destination_alarm_type_id = get_respective_alarm_type_id_from_destination(dest_conn, source_alarm_type)
    if not destination_alarm_type_id:
        logger.warning(f"⚠️ No mapped alarm type for '{source_alarm_type}'. Using predefined alarm type ID 'acf15f45-1c8f-426a-8b78-5dbbfef95c36'.")
        destination_alarm_type_id = "acf15f45-1c8f-426a-8b78-5dbbfef95c36"

    # 5️⃣ Fetch Media
    media_records = fetch_alarm_media_by_alarm_id(source_conn, original_raw_alarm_id)
    if not media_records:
        logger.error("❌ No media records found. Exiting.")
        return
    logger.info("✅ Media record fetched.")

    # 6️⃣ Prepare raw alarm
    new_alarm_data = prepare_new_raw_alarm_data(
        original_alarm_data=original_alarm,
        new_camera_id=new_camera_id,
        new_tenant_id=new_tenant_id,
        alarm_type_id=destination_alarm_type_id,
        new_door_id=new_door_id,
        new_employee_id=new_employee_id,
        new_latest_media_id=None
    )
    new_alarm_id = new_alarm_data['id']
    state.add_alarm(new_alarm_id)

    # 7️⃣ Insert media
    media_id = insert_alarm_media_and_get_id(dest_conn, media_records[0], new_alarm_id)
    if not media_id:
        logger.error("❌ Media insert failed. Exiting.")
        return
    logger.info(f"✅ Media inserted with ID: {media_id}")
    new_alarm_data['latest_alarm_media_id'] = media_id

    # 8️⃣ Handle ML Output and Video Tags
    original_ml_output_id = original_alarm.get('ml_output_id')
    if original_ml_output_id:
        ml_output_data = fetch_ml_output_by_id(source_conn, original_ml_output_id)
        if ml_output_data:
            video_tags = fetch_video_tags_by_ml_output_id(source_conn, original_ml_output_id)
            new_ml_output_id = insert_ml_output_to_destination(dest_conn, ml_output_data, new_alarm_id, new_tenant_id)
            if new_ml_output_id:
                insert_video_tags_to_destination(dest_conn, video_tags, new_ml_output_id, new_tenant_id)
                new_alarm_data['ml_output_id'] = new_ml_output_id
                logger.info(f"ML Output and {len(video_tags)} video tags migrated.")

    # 9️⃣ Handle Alarm Updates + Users
    alarm_updates = fetch_alarm_updates(source_conn, original_raw_alarm_id)
    last_update_id = None
    if alarm_updates:
        last_update_id = insert_alarm_updates(dest_conn, source_conn, alarm_updates, new_alarm_id, new_tenant_id)
        if last_update_id:
            new_alarm_data['alarm_update_id'] = last_update_id
            logger.info(f"Alarm updates migrated. Last update ID: {last_update_id}")

    # 🔟 Add migrated IDs to raw alarm
    new_alarm_data['door_id'] = new_door_id
    new_alarm_data['employee_id'] = new_employee_id
    new_alarm_data['latest_alarm_media_id'] = media_id
    new_alarm_data['alarm_update_id'] = last_update_id
    new_alarm_data['ml_output_id'] = new_alarm_data.get('ml_output_id')

    # 1️⃣1️⃣ Insert Raw Alarm
    logger.info(f"📥 Inserting raw alarm with ID: {new_alarm_id}")
    success = insert_raw_alarm(dest_conn, new_alarm_data)
    if success:
        logger.info(f"✅ Raw alarm inserted successfully with ID: {new_alarm_id}")
    else:
        logger.error("❌ Failed to insert raw alarm.")


    source_conn.close()
    dest_conn.close()
    logger.info("✅ Migration complete. Connections closed.")

def fetch_raw_alarm_by_id(source_connection, raw_alarm_id):
    cursor = source_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM raw_alarms_v2 WHERE id = %s", (raw_alarm_id,))
    return cursor.fetchone()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migrate a raw alarm by ID')
    parser.add_argument('original_raw_alarm_id', type=str, help='The ID of the raw alarm to migrate')
    args = parser.parse_args()
    main(args.original_raw_alarm_id) 