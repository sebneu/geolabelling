

def generateFromYACPTable(url, inputTable, error=None, portalInfo=None):
    dt = {}
    dt['url'] = url
    if portalInfo is not None:
        dt['portal']= {
                    "id": portalInfo['id'],
                    "iso": portalInfo['iso'],
                    "uri": portalInfo['uri'],
                    "software": portalInfo['software'],
                    "apiuri": portalInfo['apiuri']
                }
    if error:
        dt['parsingerror']=error
        return dt

    dt['no_rows'] = inputTable.noRows
    dt['no_columns'] = inputTable.noCols
    dt['dialect'] = inputTable.meta['dialect']
    dt['dialect']['encoding'] = inputTable.meta['encoding']

    dt['column'] = []
    for i, col in enumerate(inputTable.columnIter()):
        dc={
            'col_no': i,
            'values': {'exact':col,'value':col}
            }

        nonEmpty = [x for x in col if x and len(x.strip()) > 0]
        if len(nonEmpty) > 0:
            dc['min'] = sorted(nonEmpty)[0]
            dc['max'] = sorted(nonEmpty, reverse=True)[0]

        if len(inputTable.header_cols)==inputTable.noCols:
            dc['header']={'exact': inputTable.header_cols[i], 'value': inputTable.header_cols[i]},
        dt['column'].append(dc)

    dt['row'] = []
    for i, row in enumerate(inputTable.rowIter()):
        dt['row'].append({
            'row_no': i,
            'values': {'exact': row, 'value': row}
        })
    return dt

def mapping(language='standard'):
    return {
        "table": {
            "properties": {
                "url": {"type": "keyword"},
                "portal": {
                    "type": "nested",
                    "properties":{
                        "id": {"type": "keyword"},
                        "iso": {"type": "keyword"},
                        "uri": {"type": "keyword"},
                        "software": {"type": "keyword"},
                        "apiuri": {"type": "keyword"}
                    }
                },

                "dataset": {
                    "type": "nested",
                    "properties": {
                        "dataset_name": {"type": "text", "analyzer": language},
                        "dataset_link": {"type": "keyword"},
                        "dataset_description": {"type": "text", "analyzer": language},
                        "name": {"type": "text", "analyzer": language},
                        "publisher": {"type": "text", "analyzer": language},
                        "publisher_link": {"type": "keyword"},
                        "publisher_email": {"type": "keyword"},
                        "keywords": {"type": "text", "analyzer": language}
                    }
                },
                "transaction_time": {"type": "date"},
                "metadata_temp_start": {"type": "date"},
                "metadata_temp_end": {"type": "date"},

                "data_temp_start": {"type": "date"},
                "data_temp_end": {"type": "date"},
                "data_temp_pattern": {"type": "keyword"},

                "metadata_entities": {"type": "keyword"},
                "data_entities": {"type": "keyword"},

                "no_rows": {"type": "long"},
                "no_columns": {"type": "long"},

                "parsingerror":{
                    "properties": {
                        "errorclass": {"type": "text", "index": "false"},
                        "errormessage": {"type": "text", "index": "false"}
                    }
                },

                "dialect":{
                    "type": "nested",
                    "properties": {
                        "delimiter": {"type": "keyword"},
                        "lineterminator": {"type": "keyword"},
                        "skipinitialspace": {"type": "keyword"},
                        "quoting": {"type": "keyword"},
                        "quotechar": {"type": "keyword"},
                        "doublequote": {"type": "keyword"},
                        "encoding": {"type": "keyword"}
                    }
                },

                "column": {
                    "type": "nested",
                    "properties": {
                        "header": {
                            "type": "object",
                            "properties": {
                                "exact": {"type": "keyword"},
                                "value": {"type": "text", "analyzer": language}
                            },
                        },
                        "col_no": {"type": "long"},
                        "min": {"type": "text", "analyzer": language},
                        "max": {"type": "text", "analyzer": language},
                        "values": {
                            "type": "object",
                            "properties": {
                                "exact": {"type": "keyword"},
                                "value": {"type": "text", "analyzer": language}
                            }
                        },
                        "entities": {"type": "keyword"},
                        "source": {"type": "keyword"},
                        "dates": {"type": "date"}
                    }
                },

                "row": {
                    "type": "nested",
                    "properties": {
                        "row_no": {"type": "long"},
                        "values": {
                            "type": "object",
                            "properties": {
                                "exact": {"type": "keyword"},
                                "value": {"type": "text", "analyzer": language}
                            }
                        },
                        "entities": {"type": "keyword"},
                        "dates": {"type": "date"}
                    }
                }
            }
        }
    }