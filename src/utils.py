def fix_datetime(o):
    """
    Fix datetime objects for JSON serialization

    param:
        o: object to fix

    Example:

        json_str = gdf.to_json(default=fix_obj)
        with open(path, 'w') as f:
            f.write(json_str)
    """
    if hasattr(o, "isoformat"):
        return o.isoformat()
    raise TypeError