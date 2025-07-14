from src.utils.logger import get_logger
logger = get_logger("door_service")

def fetch_door_data_by_id(source_connection, door_id):
    cursor = source_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM doors WHERE id = %s", (door_id,))
    door_data = cursor.fetchone()
    return door_data


def insert_door_data_to_destination(destination_connection, door_data, new_door_id):
    cursor = destination_connection.cursor()

    door_data['id'] = new_door_id  
    door_data['tenant_id'] = 'demo-sales'  
    door_data['location_id'] = '1375'  
    columns = ', '.join(door_data.keys())
    placeholders = ', '.join(['%s'] * len(door_data))
    query = f"INSERT INTO doors ({columns}) VALUES ({placeholders})"
    cursor.execute(query, list(door_data.values()))
    destination_connection.commit()
    return True


def get_door_from_destination(destination_connection, door_data):
    cursor = destination_connection.cursor(dictionary=True)
    query = "SELECT * FROM doors WHERE tenant_id = %s AND location_id = %s AND door_name = %s"
    cursor.execute(query, ('demo-sales', '1375', door_data['door_name']))
    door = cursor.fetchone()

    if door:
        logger.info(f"Door with name '{door_data['door_name']}' already exists in the destination database with ID: {door['id']}.")
        return door
    else:
        logger.info(f"Door with name '{door_data['door_name']}' does not exist in the destination database.")
        return None 