import json
import uuid
from src.services.user_service import fetch_user_by_id, insert_user_if_not_exists
from src.utils.logger import get_logger
logger = get_logger("alarm_update_service")
# ... existing code ...
def fetch_alarm_updates(source_conn, original_alarm_id):
    try:
        cursor = source_conn.cursor(dictionary=True)
        query = "SELECT * FROM alarm_updates WHERE alarm_id = %s"
        cursor.execute(query, (original_alarm_id,))
        updates = cursor.fetchall()

        if updates:
            logger.info(f"Found {len(updates)} alarm update(s) for alarm ID {original_alarm_id}")
        else:
            logger.info(f"No alarm updates found for alarm ID {original_alarm_id}")

        return updates

    except Exception as e:
        logger.error(f"Error fetching alarm_updates: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
# ... existing code ...
def insert_alarm_updates(dest_conn, source_conn, updates, new_alarm_id, new_tenant_id):
    if not updates:
        logger.info("No updates to insert.")
        return None

    cursor = None
    last_inserted_id = None

    try:
        cursor = dest_conn.cursor()

        for update in updates:
            new_update_id = str(uuid.uuid4())

            old_user_id = update.get('user_id')
            new_user_id = None

            if old_user_id:
                user_record = fetch_user_by_id(source_conn, old_user_id)
                if user_record:
                    new_user_id = insert_user_if_not_exists(dest_conn, user_record, new_tenant_id)

            update_details = update.get('update_details')
            if isinstance(update_details, (dict, list)):
                update_details = json.dumps(update_details)

            query = """
                INSERT INTO alarm_updates (
                    id, alarm_id, update_timestamp_utc, event, user_id,
                    plain_text_comment, current_status, tenant_id, update_details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                new_update_id,
                new_alarm_id,
                update['update_timestamp_utc'],
                update['event'],
                new_user_id,  
                update['plain_text_comment'],
                update['current_status'],
                new_tenant_id,
                update_details
            )

            cursor.execute(query, values)
            logger.info(f"Inserted alarm update with new ID {new_update_id}")
            last_inserted_id = new_update_id

        dest_conn.commit()
        logger.info(f"Committed {len(updates)} alarm update(s) for alarm ID {new_alarm_id}")
        return last_inserted_id

    except Exception as e:
        logger.error(f"Error inserting alarm_updates: {e}")
        dest_conn.rollback()
        return None

    finally:
        if cursor:
            cursor.close()
# ... existing code ...