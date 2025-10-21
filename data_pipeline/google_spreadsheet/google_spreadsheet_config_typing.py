from typing import Sequence
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.pipeline_config_typing import StateFileConfigDict


class GoogleSpreadsheetSheetConfigDict(TypedDict):
    sheetName: str
    tableName: str
    datasetName: str
    tableWriteAppend: NotRequired[bool]
    headerLineIndex: NotRequired[int]
    sheetRange: NotRequired[str]


class BaseGoogleSpreadsheetConfigDict(TypedDict):
    spreadsheetId: str
    sheets: Sequence[GoogleSpreadsheetSheetConfigDict]
    dataPipelineId: str
    stateFile: NotRequired[StateFileConfigDict]


class InheritedGoogleSpreadsheetConfigDict(TypedDict):
    gcpProjectName: str
    importedTimestampFieldName: str


class GoogleSpreadsheetConfigDict(
    BaseGoogleSpreadsheetConfigDict,
    InheritedGoogleSpreadsheetConfigDict
):
    pass


class MultiGoogleSpreadsheetConfigDict(TypedDict):
    gcpProjectName: str
    importedTimestampFieldName: str
    spreadsheets: Sequence[BaseGoogleSpreadsheetConfigDict]
