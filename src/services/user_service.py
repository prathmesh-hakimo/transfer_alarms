import json
import uuid
from src.utils.logger import get_logger
logger = get_logger("user_service")

def fetch_user_by_id(connection, user_id):
    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM users WHERE id = %s"
        cursor.execute(query, (user_id,))
        user = cursor.fetchone()

        if user:
            logger.info(f"Found user with ID: {user_id}")
        else:
            logger.info(f"No user found with ID: {user_id}")

        return user

    except Exception as e:
        logger.error(f"Error fetching user: {e}")
        return None

    finally:
        if cursor:
            cursor.close()

def get_user_id_by_email(dest_conn, email):
    cursor = dest_conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
        result = cursor.fetchone()
        return result['id'] if result else None
    finally:
        cursor.close()

def user_exists_in_destination(dest_conn, old_user_id):
    try:
        cursor = dest_conn.cursor()
        cursor.execute("SELECT id FROM users WHERE id = %s", (old_user_id,))
        return cursor.fetchone() is not None
    except Exception as e:
        logger.warning(f"Error checking user existence: {e}")
        return False
    finally:
        cursor.close()

def insert_user_if_not_exists(dest_conn, user_record, new_tenant_id):
    old_user_id = user_record['id']
    user_email = user_record['email']

    if user_exists_in_destination(dest_conn, old_user_id):
        logger.info(f"User {old_user_id} already exists in destination. Using same ID.")
        return old_user_id

    existing_user_id_by_email = get_user_id_by_email(dest_conn, user_email)
    if existing_user_id_by_email:
        logger.info(f"User with email '{user_email}' already exists. Using existing ID: {existing_user_id_by_email}")
        return existing_user_id_by_email

    new_user_id = str(uuid.uuid4())

    try:
        cursor = dest_conn.cursor()

        msp_tenants = json.dumps(user_record.get('msp_tenants')) if user_record.get('msp_tenants') else None
        msp_locations = json.dumps(user_record.get('msp_locations')) if user_record.get('msp_locations') else None
        vision_tenants = json.dumps(user_record.get('vision_tenants')) if user_record.get('vision_tenants') else None

        query = """
            INSERT INTO users (
                id, name, email, is_enabled, password, tenant_id, 
                created_at_utc, updated_at_utc, refresh_token, refresh_token_expires, 
                role_id, msp_tenants, msp_locations, vision_tenants
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            new_user_id,
            user_record.get('name'),
            user_email,
            user_record.get('is_enabled', 1),
            user_record.get('password'),
            new_tenant_id,
            user_record.get('created_at_utc'),
            user_record.get('updated_at_utc'),
            user_record.get('refresh_token'),
            user_record.get('refresh_token_expires'),
            user_record.get('role_id'),
            msp_tenants,
            msp_locations,
            vision_tenants
        )

        cursor.execute(query, values)
        dest_conn.commit()
        logger.info(f"Inserted user {user_email} with new ID {new_user_id}")
        return new_user_id

    except Exception as e:
        logger.error(f"Error inserting user: {e}")
        dest_conn.rollback()
        return None

    finally:
        cursor.close() 