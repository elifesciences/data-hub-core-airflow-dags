import xml.etree.ElementTree as ET

from data_pipeline.utils.xml import parse_xml_and_return_it_as_dict


class TestParseXmlAndReturnItAsDict:
    def test_should_return_empty_list_if_xml_root_is_empty(self):
        xml_root = ET.fromstring('<empty_root></empty_root>')
        return_value = parse_xml_and_return_it_as_dict(xml_root)
        assert return_value == {'empty_root': {}}
