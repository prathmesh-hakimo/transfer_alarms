def fetch_alarm_media_by_alarm_id(connection, alarm_id):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM alarm_media WHERE alarm_id = %s", (alarm_id,))
    media_records = cursor.fetchall()
    if len(media_records) != 1:
        print(f" Warning: Expected 1 media record, found {len(media_records)}")
    return media_records


def insert_alarm_media_and_get_id(connection, media_record, new_alarm_id):
    cursor = connection.cursor()
    query = """
        INSERT INTO alarm_media (alarm_id, media_type, media_url, created_at_utc, updated_at_utc)
        VALUES (%s, %s, %s, %s, %s)
    """
    values = (
        new_alarm_id,
        media_record['media_type'],
        media_record['media_url'],
        media_record['created_at_utc'],  
        media_record['updated_at_utc'],  
    )
    cursor.execute(query, values)
    connection.commit()
    return cursor.lastrowid 