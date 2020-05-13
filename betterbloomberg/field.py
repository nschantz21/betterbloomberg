import blpapi
import pandas as pd
from .core import BlpDataRequest

__all__ = ["FieldInfo", "FieldSearch"]

class FieldRequest(BlpDataRequest):
    service_type = "//blp/apiflds"


class FieldInfo(FieldRequest):
    request_type = "FieldInfoRequest"

    def __init__(self, field_id, docs=False, overrides=False, verbose=False, **kwargs):
        """
        Field Information Request: Provides a description of the specified fields
        in the request.

        Parameters
        ----------
            field_id : string
                Fields can be specified as an alpha numeric or mnemonic.
            docs : bool
                Returns a description about the field as seen on FLDS <GO>. Default
                value is false.
            overrides : bool
                Returns a value for the element that describes the behavior of the
                field requested.  It will give a list of overrides for that field
        """

        self.field_id = field_id
        self.docs = docs
        self.overrides = overrides
        self.verbose = verbose
        super(FieldRequest, self).__init__(**kwargs)

    def generate_request(self):
        for fid in self.field_id:
            self.request.append("id", fid)
        self.request.set("returnFieldDocumentation", self.docs)
        if self.overrides:
            self.request.append("properties", "fieldoverridable")

    def process_response(self):
        field_dict = dict()

        SECURITY_DATA = blpapi.Name("fieldData")
        FIELD_DATA = blpapi.Name("fieldInfo")

        securityData = (
            blpapi.event.MessageIterator(self.response).next().getElement(SECURITY_DATA)
        )

        sub_fields = ["mnemonic", "description"]
        if self.docs:
            sub_fields.append("documentation")

        for i in range(securityData.numValues()):
            tmp_sec = securityData.getValueAsElement(i)
            fid = tmp_sec.getElementAsString("id")
            field_dict[fid] = dict()
            for f in sub_fields:
                field_dict[fid][f] = tmp_sec.getElement(FIELD_DATA).getElementAsString(
                    f
                )
            if self.overrides:
                ovrd_list = list()
                for j in range(
                    tmp_sec.getElement(FIELD_DATA).getElement("overrides").numValues()
                ):
                    ovrd_list.append(
                        tmp_sec.getElement(FIELD_DATA)
                        .getElement("overrides")
                        .getValue(j)
                    )
                field_dict[fid]["overrides"] = ovrd_list

        return field_dict

    @property
    def data(self):
        if self.use_pandas:
            return pd.DataFrame.from_dict(self.__data, orient="index")
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data


class FieldSearch(FieldRequest):
    request_type = "FieldSearchRequest"

    categories = {
        "NewFields",
        "Analysis",
        "Corporate Actions",
        "Custom Fields",
        "Descriptive",
        "Earnings",
        "Estimates",
        "Fundamentals",
        "Market Activity",
        "Metadata",
        "Ratings",
        "Trading",
        "Systems",
    }

    product_types = {
        "All",
        "Govt",
        "Corp",
        "Mtge",
        "M-Mkt",
        "Muni",
        "Pfd",
        "Equity",
        "Cmdty",
        "Index",
        "Curncy",
    }

    field_type = {"All", "RealTime", "Static"}

    bps_requirement = {"All", "BPS", "NoBPS"}

    def __init__(
        self,
        query,
        docs=True,
        inc_product_type=None,
        inc_categories=None,
        inc_field_type=None,
        inc_bps_requirement="All",
        exc_product_type=None,
        exc_categories=None,
        exc_field_type=None,
        exc_bps_requirement=None,
        lang="ENGLISH",
        **kwargs
    ):
        """Field Search Request"""
        self.query = query
        self.docs = docs
        self.inc_product_type = inc_product_type
        self.inc_categories = inc_categories
        self.inc_field_type = inc_field_type
        self.inc_bpsRequirement = inc_bps_requirement
        self.exc_product_type = exc_product_type
        self.exc_categories = exc_categories
        self.exc_field_type = exc_field_type
        self.exc_bpsRequirement = exc_bps_requirement
        self.lang = lang
        super(FieldSearch, self).__init__(**kwargs)

    def generate_request(self):
        self.request.set("searchSpec", self.query)
        self.request.set("returnFieldDocumentation", self.docs)
        self.request.set("language", self.lang)

        # includes
        includes = self.request.getElement("include")
        if self.inc_product_type is not None:
            includes.setElement("productType", self.inc_product_type)
        if self.inc_field_type is not None:
            includes.setElement("fieldType", self.inc_field_type)
        if self.inc_categories is not None:
            includes_cats = includes.getElement("category")
            for cat in self.inc_categories:
                includes_cats.appendValue(cat)
        if self.inc_bpsRequirement is not None:
            includes.setElement("bpsRequirement", self.inc_bpsRequirement)

        # excludes
        excludes = self.request.getElement("exclude")
        if self.exc_product_type is not None:
            excludes.setElement("productType", self.exc_product_type)
        if self.exc_field_type is not None:
            excludes.setElement("fieldType", self.exc_field_type)
        if self.exc_categories is not None:
            excludes_cats = excludes.getElement("category")
            for cat in self.exc_categories:
                excludes_cats.appendValue(cat)
        if self.exc_bpsRequirement is not None:
            excludes.setElement("bpsRequirement", self.exc_bpsRequirement)

    def process_response(self):
        field_dict = dict()

        SECURITY_DATA = blpapi.Name("fieldData")
        FIELD_DATA = blpapi.Name("fieldInfo")

        securityData = (
            blpapi.event.MessageIterator(self.response).next().getElement(SECURITY_DATA)
        )

        sub_fields = ["mnemonic", "description", "categoryName"]
        if self.docs:
            sub_fields.append("documentation")

        for i in range(securityData.numValues()):
            tmp_sec = securityData.getValueAsElement(i)
            fid = tmp_sec.getElementAsString("id")
            field_dict[fid] = dict()
            for f in sub_fields:
                field_dict[fid][f] = tmp_sec.getElement(FIELD_DATA).getElementAsString(
                    f
                )
                ovrd_list = list()
                for j in range(
                    tmp_sec.getElement(FIELD_DATA).getElement("overrides").numValues()
                ):
                    ovrd_list.append(
                        tmp_sec.getElement(FIELD_DATA)
                        .getElement("overrides")
                        .getValue(j)
                    )
                field_dict[fid]["overrides"] = ovrd_list

        return field_dict

    @property
    def data(self):
        if self.use_pandas:
            return pd.DataFrame.from_dict(self.__data, orient="index")
        else:
            return self.__data

    @data.setter
    def data(self, value):
        self.__data = value

    @data.deleter
    def data(self):
        del self.__data
