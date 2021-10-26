import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri

from rpy2.robjects.conversion import localconverter
# 'dta <- read.csv(paste("/Users/dyc/Downloads/sample_data_collect_na.csv",sep=""), header=T);' \


def scci_given_data(df, effect, cause, z_list):
    effect_df = df[effect]
    cause_df = df[cause]
    if len(z_list) == 0:
        df['z0'] = 1
        condition_df = df['z0']
    else:
        condition_df = df[z_list]
    SCCI = importr('SCCI')
    with localconverter(ro.default_converter + pandas2ri.converter):
        x = ro.conversion.py2rpy(effect_df)
        y = ro.conversion.py2rpy(cause_df)
        z = ro.conversion.py2rpy(condition_df)
        scci = SCCI.SCCI(x=x, y=y, Z=z, score="fNML", sym=False)
    # coder = 'library(SCCI); ' \
    #         'dta <- read.csv(paste("../cb_df.csv",sep=""), header=T);' \
    #         'x <- dta["y"]; ' \
    #         'y <- dta["x"]; ' \
    #         'z <-dta[c("z1", "z2" )]; ' \
    #         'scci <- SCCI(x=x, y=y, Z=z, score="fNML", sym=FALSE); '
    return float(scci)


if __name__ == "__main__":
    df = pd.DataFrame({
        'y': [1, 0, 1],
        'x': [1, 0, 1],
        'z1': [0, 1, 1]
    })
    print(scci_given_data(df, 'y', 'x', ['z1']))