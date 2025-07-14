class DuplicationState:
    """
    Tracks the state of the camera and alarm duplication process.
    """
    def __init__(self):
        self.original_camera_ids = []
        self.new_camera_ids = []
        self.camera_names = []
        self.new_alarm_ids = []
        self.location_alarm_count = 0
        self.location_alarm_ids = []
        self.sop_count = 0
        self.sop_ids = []
        self.location_contact_count = 0
        self.location_contact_ids = []

    def add_camera(self, original_id, new_id, name):
        self.original_camera_ids.append(original_id)
        self.new_camera_ids.append(new_id)
        self.camera_names.append(name)

    def add_alarm(self, new_alarm_id):
        self.new_alarm_ids.append(new_alarm_id)
        self.location_alarm_count += 1
        self.location_alarm_ids.append(new_alarm_id)

    def add_sop(self, sop_id):
        self.sop_count += 1
        self.sop_ids.append(sop_id)

    def add_location_contact(self, contact_id):
        self.location_contact_count += 1
        self.location_contact_ids.append(contact_id)

    def get_summary(self):
        return {
            'original_camera_ids': self.original_camera_ids,
            'new_camera_ids': self.new_camera_ids,
            'camera_names': self.camera_names,
            'new_alarm_ids': self.new_alarm_ids,
            'location_alarm_count': self.location_alarm_count,
            'location_alarm_ids': self.location_alarm_ids,
            'sop_count': self.sop_count,
            'sop_ids': self.sop_ids,
            'location_contact_count': self.location_contact_count,
            'location_contact_ids': self.location_contact_ids,
        } 