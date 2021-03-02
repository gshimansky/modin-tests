if __name__ == "__main__":
    import numpy as np
    import modin.pandas as pd

    dtypes = {
        'object_id': 'int32',
        'mjd': 'float32',
        'passband': 'int32',
        'flux': 'float32',
        'flux_err': 'float32',
        'detected': 'int32'
    }
    df = pd.read_csv('training_set_100000.csv', dtype=dtypes)
    print(df)
    print(df['flux'][12724])

    flux = df['flux']
    flux2 = np.power(flux, np.float32(1.0))
    # flux2.name = "flux"
    print(flux)
    flux.to_csv("modin1.csv")
    print(flux2)
    flux2.to_csv("modin2.csv")
    print("EQUALS = ", flux2.equals(flux))
    df['flux_ratio_sq'] = flux2
    print(df['flux_ratio_sq'][12724])
    df.to_csv("modin.csv", index=False)

    df['flux_by_flux_ratio_sq'] = df['flux'] * df['flux_ratio_sq']
    print(df)
