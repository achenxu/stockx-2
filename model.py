from pandas import read_csv
from pandas import datetime
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

df = read_csv('shoe_data/shoe_transactions/adidas/adidas-zx-500-dragon-ball-z-son-goku.csv')
print(df.columns)
test_df = df.loc[df["shoeSize"] == 9, "amount"]
df = df.loc[df["shoeSize"] == 10, "amount"]
size = int(df.shape[0] * 0.8)

train, test = list(df[:size]), list(df[size+1:df.shape[0]])
print(len(train), len(test))

history = [x for x in train]

history = [x for x in list(df)]

predictions = []
for i in range(len(list(test_df))):
	model = ARIMA(history, order=(3,1,0))	
	model_fit = model.fit(disp=0)
	output = model_fit.forecast()
	yhat = output[0]
	predictions.append(yhat)
	obs = list(test_df)[i]
	history.append(obs)
	print('predicted=%f, expected=%f' % (yhat, obs))
error = mean_squared_error(test, predictions)
print('Test MSE: %.3f' % error)
# plot
pyplot.plot(test)
pyplot.plot(predictions, color='red')
pyplot.show()