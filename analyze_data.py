from store_data import pull

if __name__ == "__main__":

    # There are 1+ dummy lines

    data = pull()
    data_spy = data[data["ticker"] == "SPY"]
    data_qqq = data[data["ticker"] == "QQQ"]
    data_iwm = data[data["ticker"] == "IWM"]

    print("data_spy.shape:", data_spy.shape)
    print("data_qqq.shape:", data_qqq.shape)
    print("data_iwm.shape:", data_iwm.shape)

    """"
    IDEAS 
    
    1. Possibly track the same strike the whole day
    2. Find a function to get Top of Book MKT data for options too

    """