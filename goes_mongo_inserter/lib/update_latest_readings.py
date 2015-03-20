

def update_env_latest(db, config, data):
    """ Finds the latest value for each reading and stores the
        key, value, and timestamp
    """

    collection_name = "%s.env.latest" % config['station']
    collection = db[collection_name]

    state = {}
    for d in data:
        for key in d:
            key_parts = key.split('-')
            parameter = key_parts[0]
            if len(key_parts) > 1:
                units = key_parts[1]
            else:
                units = None

            state[key] = {
                'timestamp': d['timestamp'],
                'value': d[key],
                'parameter': parameter,
                'units': units
            }

    collection.update({}, {'$set': state}, upsert=True)
