from typing import Any, Dict
from xml.etree.ElementTree import Element


def parse_xml_and_return_it_as_dict(
    xml_root: Element,
    is_root: bool = True
) -> dict:
    if is_root:
        return {xml_root.tag: parse_xml_and_return_it_as_dict(
            xml_root,
            is_root=False
        )}
    dict_of_xml: Dict[Any, Any] = xml_root.attrib
    if xml_root.text:
        dict_of_xml["value_text"] = xml_root.text
    for xml_child in xml_root:
        if xml_child.tag not in dict_of_xml:
            dict_of_xml[xml_child.tag] = []
        dict_of_xml[xml_child.tag].append(parse_xml_and_return_it_as_dict(
            xml_child,
            is_root=False
        ))
    return dict_of_xml
