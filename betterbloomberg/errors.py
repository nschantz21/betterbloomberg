from collections import namedtuple

SecurityError = namedtuple("SecurityError", ["sec_id", "source", "code", "category", "message", "subcategory"])
FieldErrorAttrs = ['security', 'field', 'source', 'code', 'category', 'message', 'subcategory']
FieldError = namedtuple('FieldError', FieldErrorAttrs)

def to_security_error(sec_id, element):
    return SecurityError(
        sec_id,
        element.getElement("source").getValue(),
        element.getElement("code").getValue(),
        element.getElement("category").getValue(),
        element.getElement("message").getValue(),
        element.getElement("subcategory").getValue()
    )


def as_field_error(sec_id, node):
    """ convert a fieldExceptions element to a FieldError or FieldError array """
    assert node.name() == 'fieldExceptions'
    if node.isArray():
        return [as_field_error(sec_id, node.getValue(_)) for _ in range(node.numValues())]
    else:
        #print("field_error", node)
        fld = node.getElement('fieldId').getValue()
        info = node.getElement('errorInfo')
        src = info.getElement("source").getValue()
        code = info.getElement("code").getValue()
        cat = info.getElement("category").getValue()
        msg = info.getElement("message").getValue()
        subcat = info.getElement("subcategory").getValue()
        return FieldError(security=sec_id, field=fld, source=src, code=code, category=cat, message=msg,
                          subcategory=subcat)
