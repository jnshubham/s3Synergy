from pandas import DataFrame, read_csv
from io import StringIO
import json
import boto3
from typing import List
import re
from exceptions import InvalidFormatException, HeaderNotInQueryException

class S3Synergy:
    '''Fetch the data based on the key provided and returns the output'''
    def __init__(self):
        self._reserved_keywords = ['absolute', 'action', 'add', 'all', 'allocate', 'alter', 'and', 'any', 'are', 'as', 'asc', 'assertion', 'at', 'authorization', 'avg', 'bag', 'begin', 'between', 'bit', 'bit_length', 'blob', 'bool', 'boolean', 'both', 'by', 'cascade', 'cascaded', 'case', 'cast', 'catalog', 'char', 'char_length', 'character', 'character_length', 'check', 'clob', 'close', 'coalesce', 'collate', 'collation', 'column', 'commit', 'connect', 'connection', 'constraint', 'constraints', 'continue', 'convert', 'corresponding', 'count', 'create', 'cross', 'current', 'current_date', 'current_time', 'current_timestamp', 'current_user', 'cursor', 'date', 'day', 'deallocate', 'dec', 'decimal', 'declare', 'default', 'deferrable', 'deferred', 'delete', 'desc', 'describe', 'descriptor', 'diagnostics', 'disconnect', 'distinct', 'domain', 'double', 'drop', 'else', 'end', 'end-exec', 'escape', 'except', 'exception', 'exec', 'execute', 'exists', 'external', 'extract', 'false', 'fetch', 'first', 'float', 'for', 'foreign', 'found', 'from', 'full', 'get', 'global', 'go', 'goto', 'grant', 'group', 'having', 'hour', 'identity', 'immediate', 'in', 'indicator', 'initially', 'inner', 'input', 'insensitive', 'insert', 'int', 'integer', 'intersect', 'interval', 'into', 'is', 'isolation', 'join', 'keyPPPPP', 'language', 'last', 'leading', 'left', 'level', 'like', 'limit', 'list', 'local', 'lower', 'match', 'max', 'min', 'minute', 'missing', 'module', 'month', 'names', 'national', 'natural', 'nchar', 'next', 'no', 'not', 'null', 'nullif', 'numeric', 'octet_length', 'of', 'on', 'only', 'open', 'option', 'or', 'order', 'outer', 'output', 'overlaps', 'pad', 'partial', 'pivot', 'position', 'precision', 'prepare', 'preserve', 'primary', 'prior', 'privileges', 'procedure', 'public', 'read', 'real', 'references', 'relative', 'restrict', 'revoke', 'right', 'rollback', 'rows', 'schema', 'scroll', 'second', 'section', 'select', 'session', 'session_user', 'set', 'sexp', 'size', 'smallint', 'some', 'space', 'sql', 'sqlcode', 'sqlerror', 'sqlstate', 'string', 'struct', 'substring', 'sum', 'symbol', 'system_user', 'table', 'temporary', 'then', 'time', 'timestamp', 'timezone_hour', 'timezone_minute', 'to', 'trailing', 'transaction', 'translate', 'translation', 'trim', 'true', 'tuple', 'union', 'unique', 'unknown', 'unpivot', 'update', 'upper', 'usage', 'user', 'using', 'value', 'values', 'varchar', 'varying', 'view', 'when', 'whenever', 'where', 'with', 'work', 'write', 'year', 'zone']
        self._session = boto3.client('s3')

    def readData(self, key: str,
                 format: str,
                 header_flag: str = 'Y',
                 headers: str = None,
                 sep: str = ',',
                 query: str = '',
                 lines: int=0,
                 compression: str = '',
                 quoteChar: str = '') -> DataFrame:
        '''Read data in the required format and returns the dataframe'''
        if(format.lower() == 'csv'):
            if(query == ''):
                return S3Synergy._get_csv_data(self, key, lines, sep, headers),None,None
            else:
                query = S3Synergy._generate_comatible_SQL(self, query, header_flag, headers, sep, lines)
                data, stats= S3Synergy._get_csv_data_query(self, key, query, sep, header_flag, compression, quoteChar)
                df = S3Synergy._jsonToDF(data, header_flag, headers)
                return df, stats['BytesScanned'], stats['BytesProcessed']

        elif(format.lower() == 'json'):
            data, stats = S3Synergy._get_data(self, key, query, lines, 'json')
            df = S3Synergy._jsonToDF(data)
            return df, stats['BytesScanned'], stats['BytesProcessed']

        elif(format.lower() == 'parquet'):
            data, stats = S3Synergy._get_data(self, key, query, lines, 'parquet')
            # print(data,stats)
            df = S3Synergy._jsonToDF(data)
            return df, stats['BytesScanned'], stats['BytesProcessed']

        else:
            raise InvalidFormatException('Invalid Format spotted.. Supported Types are csv, json or parquet')

    def _get_csv_data(self, key: str, lines: int, sep: str, header) -> DataFrame:
        data = S3Synergy._get_StringBufferFromS3(self, key, lines+1)
        if(header is not None):
            df = read_csv(StringIO('\n'.join(data), sep=sep, dtype=object))
        else:
            df = read_csv(StringIO('\n'.join(data), sep=sep, header=header, dtype=object))
        return df

    def _get_StringBufferFromS3(self, key: str, lines: int) -> List:
        s3 = self._session
        bucket,sp,fileName = key[5:].partition('/')
        del(sp)
        response = s3.get_object(
            Bucket=bucket,
            Key=fileName
        )

        dataList = []
        for line in response['Body'].iter_lines():
            if(lines):
                dataList.append(line.decode('utf-8'))
                lines-=1
            else:
                break
        return dataList
               
    def _get_data(self, key: str, query: str, lines: int, format: str):
        bucket,sp,fileName = key[5:].partition('/')
        del(sp)
        input_serialization = {}
        if(format.upper()=='JSON'):
            input_serialization['JSON']={}
        elif(format.upper()=='PARQUET'):
            input_serialization['Parquet']={}
        return S3Synergy._run_s3_select(self, bucket, fileName, query, input_serialization)

    def _run_s3_select(self, bucket, key, query, input_serialization):
        s3 = self._session
        response = s3.select_object_content(
        Bucket = bucket,
        Key = key,
        ExpressionType = 'SQL',
        Expression = query,
        InputSerialization = input_serialization,
        OutputSerialization = {'JSON': {'RecordDelimiter': ','}},
        )
        records = []
        for event in response['Payload']:
            if 'Records' in event:
                records.append(event['Records']['Payload'].decode('utf-8'))
            if 'Stats' in event:
                statsDetails = event['Stats']['Details']
        return ''.join(records), statsDetails

    def _get_csv_data_query(self, key, query, sep, header, compression, quoteChar):
        '''To Be Implemented'''
        bucket,sp,fileName = key[5:].partition('/')
        del(sp)
        header = 'Use' if header.upper()=='Y' and compression=='' else 'NONE'
        
        input_serialization = {'CSV' : {
            'FileHeaderInfo': header,
            'FieldDelimiter': sep,
        }}
        if(quoteChar!=''):
            input_serialization['CSV']['QuoteCharacter'] = quoteChar
        if(compression!=''):
            input_serialization['CompressionType'] = compression.upper()

        return S3Synergy._run_s3_select(self, bucket, fileName, query, input_serialization)

    def _generate_comatible_SQL(self, query, header_flag, headers, sep, lines) -> str:
        if(header_flag.upper() == 'Y'):
            '''check for reserved keywords'''
            '''Implement reserved keyword logic later on'''
            return query
        elif(header_flag.upper() == 'N' and headers is not None):
            '''perform header replacements'''
            try:
                headers = headers.split(',')
                query_converted = re.sub(
                    r'\`(.+?)\`',
                    lambda m: 's._'+str(headers.index(m.group(0).strip('`'))+1),
                     query, flags=re.IGNORECASE)
                query_converted = re.sub(r'from (.\w+)',lambda m: m.group(0).strip('`')+' s',query_converted)
                return query_converted
            except ValueError as E:
                raise HeaderNotInQueryException('Columns selected/Filtered in Query not available in headers'+str(E)) 
        elif(header_flag.upper() == 'N' and headers==''):
            return query

    @staticmethod
    def _jsonToDF(data: str, header_flag: str='N', headers = None) -> DataFrame:
        df = DataFrame(json.loads('['+data.strip(',')+']'))
        if(headers is not None):
            headers = headers.split(',')
            df.columns=[headers[int(col.split('_')[1])-1] for col in df.columns.tolist()]
        return df

