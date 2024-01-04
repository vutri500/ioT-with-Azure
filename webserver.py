from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import io
from flask import Flask, render_template, send_file, make_response, request
import sqlite3

app = Flask(__name__)

def connect_db():
    # Function to connect to the SQLite database
    conn = sqlite3.connect('sensordata.db')
    curs = conn.cursor()
    return conn, curs

# Retrieve LAST data from database
def getLastData():
    # Function to retrieve the last recorded data from the database
    conn, curs = connect_db()
    for row in curs.execute("SELECT * FROM pending_data ORDER BY rasptimestamp DESC LIMIT 1"):
        time = str(row[4])
        temp = row[1]
        hum = row[2]
        pres = row[3]
    conn.close()
    return time, temp, hum, pres

def getHistData(numSamples):
    # Function to retrieve historical data from the database
    conn, curs = connect_db()
    curs.execute("SELECT * FROM pending_data ORDER BY rasptimestamp DESC LIMIT " + str(numSamples))
    data = curs.fetchall()
    dates = []
    temps = []
    hums = []
    pres = []
    for row in reversed(data):
        dates.append(row[4])
        temps.append(row[1])
        hums.append(row[2])
        pres.append(row[3])
    conn.close()
    return dates, temps, hums, pres

def maxRowsTable():
    # Function to get the maximum number of rows in the database table
    conn, curs = connect_db()
    for row in curs.execute("select COUNT(temperature) from pending_data"):
        maxNumberRows = row[0]
    conn.close()
    return maxNumberRows

# Define and initialize global variables
global numSamples
numSamples = maxRowsTable()
if numSamples > 101:
    numSamples = 100

# Main route
@app.route("/")
def index():
    # Render the index.html template with the latest data
    time, temp, hum, pres = getLastData()
    templateData = {
        'time': time,
        'temp': temp,
        'hum': hum,
        'pres': pres,
        'numSamples': numSamples
    }
    return render_template('index.html', **templateData)

@app.route('/', methods=['POST'])
def my_form_post():
    # Handle the form submission to change the number of samples
    global numSamples
    numSamples = int(request.form['numSamples'])
    numMaxSamples = maxRowsTable()
    if numSamples > numMaxSamples:
        numSamples = numMaxSamples - 1
    time, temp, hum, pres = getLastData()
    templateData = {
        'time': time,
        'temp': temp,
        'hum': hum,
        'pres': pres,
        'numSamples': numSamples
    }
    return render_template('index.html', **templateData)

@app.route('/plot/temp')
def plot_temp():
    # Generate and return the temperature plot image
    times, temps, hums, pres = getHistData(numSamples)
    ys = temps
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    axis.set_title("Temperature [°C]")
    axis.set_xlabel("Samples")
    axis.grid(True)
    xs = range(numSamples)
    axis.plot(xs, ys)
    canvas = FigureCanvas(fig)
    output = io.BytesIO()
    canvas.print_png(output)
    response = make_response(output.getvalue())
    response.mimetype = 'image/png'
    return response

@app.route('/plot/hum')
def plot_hum():
    # Generate and return the humidity plot image
    times, temps, hums, pres = getHistData(numSamples)
    ys = hums
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    axis.set_title("Humidity [%]")
    axis.set_xlabel("Samples")
    axis.grid(True)
    xs = range(numSamples)
    axis.plot(xs, ys)
    canvas = FigureCanvas(fig)
    output = io.BytesIO()
    canvas.print_png(output)
    response = make_response(output.getvalue())
    response.mimetype = 'image/png'
    return response

@app.route('/plot/pres')
def plot_pres():
    # Generate and return the pressure plot image
    times, temps, hums, pres = getHistData(numSamples)
    ys = pres
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    axis.set_title("Pressure [hPa]")
    axis.set_xlabel("Samples")
    axis.grid(True)
    xs = range(numSamples)
    axis.plot(xs, ys)
    canvas = FigureCanvas(fig)
    output = io.BytesIO()
    canvas.print_png(output)
    response = make_response(output.getvalue())
    response.mimetype = 'image/png'
    return response

if __name__ == "__main__":
    # Run the Flask app
    app.run(host='0.0.0.0', port=2704, debug=False)
