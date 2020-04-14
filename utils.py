def download_dropdown():
    return '''<button class="btn btn-secondary dropdown-toggle" type="button" id="dropdownMenuButton" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                    Download as
                    </button>'''

def generate_kwargs(data):
    arg_dict = {}
    if(validate_key(data['key'])):
        arg_dict['key'] = data['key']
        if(data['format'].lower()=='csv'):
            arg_dict['format'] = data['format']
            if(data['headers'] != ''):
                arg_dict['headers'] = data['headers']
            if(data['sep'] != ''):
                arg_dict['sep'] = data['sep']
            if(data['qc'] != ''):
                arg_dict['quoteChar'] = data['qc']
            if(data['lines'] != ''):
                arg_dict['lines'] = data['lines']
            if(data['compression'] != ''):
                arg_dict['compression'] = data['compression']
            if(data['query'] != ''):
                arg_dict['query'] = data['query']
        elif(data['format'].lower()=='json'):
            arg_dict['format'] = data['format']
            if(data['compression'] != ''):
                arg_dict['compression'] = data['compression']
            if(data['query'] != ''):
                arg_dict['query'] = data['query']
        elif(data['format'].lower()=='parquet'):
            arg_dict['format'] = data['format']
            if(data['query'] != ''):
                arg_dict['query'] = data['query']
    else:
        pass
    return arg_dict

def validate_key(key: str) -> bool:
    if(key.lower().startswith('s3://') and key.lower().rsplit('.',1)[1] in ('csv','json','parquet','gz','bz2')):
        return True
    else:
        return False