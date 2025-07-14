import uuid
import json

def fetch_ml_output_by_id(source_conn, ml_output_id):
    try:
        cursor = source_conn.cursor(dictionary=True)
        query = "SELECT * FROM ml_outputs WHERE id = %s"
        cursor.execute(query, (ml_output_id,))
        return cursor.fetchone()
    except Exception as e:
        print(f"❌ Error fetching ML Output: {e}")
        return None

def fetch_video_tags_by_ml_output_id(source_conn, ml_output_id):
    try:
        cursor = source_conn.cursor(dictionary=True)
        query = "SELECT * FROM video_tags WHERE ml_output_id = %s"
        cursor.execute(query, (ml_output_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"❌ Error fetching video tags: {e}")
        return []

def insert_ml_output_to_destination(dest_conn, ml_output_data, new_alarm_id, new_tenant_id):
    try:
        cursor = dest_conn.cursor()
        new_ml_output_id = str(uuid.uuid4())

        query = """
            INSERT INTO ml_outputs (
                id, alarm_id, true_alarm_probability, haie_ml_version,
                tenant_id, created_at_utc, updated_at_utc,
                ml_output_timestamp_utc, processed_frames, deadzone_detections
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            new_ml_output_id,
            new_alarm_id,
            ml_output_data['true_alarm_probability'],
            ml_output_data['haie_ml_version'],
            new_tenant_id,
            ml_output_data['created_at_utc'],
            ml_output_data['updated_at_utc'],
            ml_output_data['ml_output_timestamp_utc'],
            json.dumps(ml_output_data['processed_frames']) if ml_output_data['processed_frames'] else None,
            ml_output_data['deadzone_detections']
        )
        cursor.execute(query, values)
        dest_conn.commit()

        print(f"✅ Inserted ML Output with new ID: {new_ml_output_id}")
        return new_ml_output_id

    except Exception as e:
        print(f"❌ Error inserting ML Output: {e}")
        dest_conn.rollback()
        return None

def insert_video_tags_to_destination(dest_conn, video_tags, new_ml_output_id, new_tenant_id):
    try:
        cursor = dest_conn.cursor()

        for tag in video_tags:
            new_tag_id = str(uuid.uuid4())
            query = """
                INSERT INTO video_tags (
                    id, video_tag, ml_output_id, tenant_id, created_at_utc, updated_at_utc
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (
                new_tag_id,
                tag['video_tag'],
                new_ml_output_id,
                new_tenant_id,
                tag['created_at_utc'],
                tag['updated_at_utc']
            )
            cursor.execute(query, values)

        dest_conn.commit()
        print(f"✅ Inserted {len(video_tags)} video tag(s) for ML Output ID: {new_ml_output_id}")

    except Exception as e:
        print(f"❌ Error inserting video tags: {e}")
        dest_conn.rollback() 