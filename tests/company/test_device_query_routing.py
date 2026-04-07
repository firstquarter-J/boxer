import unittest

from boxer_company.routers.barcode_log import (
    _extract_device_name_scope,
    _extract_device_status_filter,
    _extract_leading_hospital_scope,
    _is_devices_filter_query_request,
    _is_recordings_filter_query_request,
)


class DeviceQueryRoutingTests(unittest.TestCase):
    def test_compound_korean_device_status_phrase_is_treated_as_device_query(self) -> None:
        question = "MB2-B00045 장비상태"

        device_name = _extract_device_name_scope(question)
        status = _extract_device_status_filter(question)

        self.assertEqual(device_name, "MB2-B00045")
        self.assertIsNone(status)
        self.assertTrue(
            _is_devices_filter_query_request(
                question,
                device_name=device_name,
                device_seq=None,
                hospital_name=None,
                room_name=None,
                hospital_seq=None,
                hospital_room_seq=None,
                status=status,
                active_flag=None,
                install_flag=None,
            )
        )

    def test_compound_korean_device_info_phrase_is_treated_as_device_query(self) -> None:
        question = "MB2-B00045 장비정보"

        device_name = _extract_device_name_scope(question)

        self.assertEqual(device_name, "MB2-B00045")
        self.assertTrue(
            _is_devices_filter_query_request(
                question,
                device_name=device_name,
                device_seq=None,
                hospital_name=None,
                room_name=None,
                hospital_seq=None,
                hospital_room_seq=None,
                status=None,
                active_flag=None,
                install_flag=None,
            )
        )

    def test_motion_cancel_voice_question_is_not_treated_as_recordings_query(self) -> None:
        question = "모션감지 종료 전 종료스캔하면 녹화 취소 음성 나와?"

        hospital_name = _extract_leading_hospital_scope(question)

        self.assertIsNone(hospital_name)
        self.assertFalse(
            _is_recordings_filter_query_request(
                question,
                barcode=None,
                target_date=None,
                target_year=None,
                hospital_name=hospital_name,
                room_name=None,
                hospital_seq=None,
                hospital_room_seq=None,
            )
        )


if __name__ == "__main__":
    unittest.main()
