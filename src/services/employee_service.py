from src.utils.logger import get_logger
logger = get_logger("employee_service")

def fetch_employee_data_by_id(source_connection, employee_id):
    cursor = source_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM employees WHERE id = %s", (employee_id,))
    employee_data = cursor.fetchone()
    return employee_data


def insert_employee_data_to_destination(destination_connection, employee_data, new_employee_id):
    cursor = destination_connection.cursor()

    employee_data['id'] = new_employee_id  
    employee_data['tenant_id'] = 'demo-sales'  
    columns = ', '.join(employee_data.keys())
    placeholders = ', '.join(['%s'] * len(employee_data))
    query = f"INSERT INTO employees ({columns}) VALUES ({placeholders})"
    cursor.execute(query, list(employee_data.values()))
    destination_connection.commit()
    return True


def get_employee_from_destination(destination_connection, employee_data):
    cursor = destination_connection.cursor(dictionary=True)
    query = "SELECT * FROM employees WHERE tenant_id = %s AND first_name = %s AND last_name = %s AND phone_number = %s"
    cursor.execute(query, ('demo-sales', employee_data['first_name'], employee_data['last_name'], employee_data['phone_number']))
    employee = cursor.fetchone()

    if employee:
        logger.info(f"Employee '{employee_data['first_name']} {employee_data['last_name']}' already exists in the destination database with ID: {employee['id']}.")
        return employee
    else:
        logger.info(f"Employee '{employee_data['first_name']} {employee_data['last_name']}' does not exist in the destination database.")
        return None 