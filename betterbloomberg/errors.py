from collections import namedtuple

SecurityError = namedtuple("SecurityError", ["sec_id", "source", "code", "category", "message", "subcategory"])


def to_security_error(sec_id, element):
    return SecurityError(
        sec_id,
        element.getElement("source").getValue(),
        element.getElement("code").getValue(),
        element.getElement("category").getValue(),
        element.getElement("message").getValue(),
        element.getElement("subcategory").getValue()
    )


def to_field_error():
    pass
