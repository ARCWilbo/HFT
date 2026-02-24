from store_data import pull

if __name__ == "__main__":

    data = pull()
    data_spy = data[data["ticker"] == "SPY"]
    data_qqq = data[data["ticker"] == "QQQ"]
    data_iwm = data[data["ticker"] == "IWM"]

    print("data_spy.shape:", data_spy.shape)
    print("data_qqq.shape:", data_qqq.shape)
    print("data_iwm.shape:", data_iwm.shape)