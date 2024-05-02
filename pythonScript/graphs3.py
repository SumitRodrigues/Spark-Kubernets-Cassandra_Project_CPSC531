import pandas as pd
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Set up the connection to Cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)
session = cluster.connect('flight_delay_analysis')

try:
    # Define queries for different analyses
    airline_delays_query = "SELECT flight_month, average_arrival_delay, average_departure_delay FROM monthly_delay_stats;"
    delay_causes_query = "SELECT marketing_airline_network, average_carrier_delay, average_weather_delay, average_nas_delay, average_security_delay, average_late_aircraft_delay FROM airline_delay_stats;"
    multiple_delay_causes_query = "SELECT marketing_airline_network, average_carrier_delay, average_weather_delay, average_nas_delay, average_security_delay, average_late_aircraft_delay, average_arrival_delay, average_departure_delay FROM airline_delay_stats;"

    # Execute queries
    airline_delays = pd.DataFrame(list(session.execute(airline_delays_query)))
    delay_causes = pd.DataFrame(list(session.execute(delay_causes_query)))
    multiple_delay_causes = pd.DataFrame(list(session.execute(multiple_delay_causes_query)))

    # Create figure and axes for subplots
    fig, axes = plt.subplots(3, 1, figsize=(12, 24))

    # Plot 1: Bar chart for monthly average arrival and departure delays
    width = 0.35  # the width of the bars
    axes[0].bar(airline_delays['flight_month'] - width/2, airline_delays['average_arrival_delay'], width, label='Average Arrival Delay')
    axes[0].bar(airline_delays['flight_month'] + width/2, airline_delays['average_departure_delay'], width, label='Average Departure Delay')
    axes[0].set_xlabel("Flight Month")
    axes[0].set_ylabel("Average Delay (minutes)")
    axes[0].set_title("Monthly Average Arrival and Departure Delays")
    axes[0].legend()
    axes[0].grid(True)

    # Plot 2: Line chart for different delay types by airline
    axes[1].plot(delay_causes['marketing_airline_network'], delay_causes['average_carrier_delay'], label='Carrier Delay', marker='o')
    axes[1].plot(delay_causes['marketing_airline_network'], delay_causes['average_weather_delay'], label='Weather Delay', marker='o')
    axes[1].plot(delay_causes['marketing_airline_network'], delay_causes['average_nas_delay'], label='NAS Delay', marker='o')
    axes[1].plot(delay_causes['marketing_airline_network'], delay_causes['average_security_delay'], label='Security Delay', marker='o')
    axes[1].plot(delay_causes['marketing_airline_network'], delay_causes['average_late_aircraft_delay'], label='Late Aircraft Delay', marker='o')
    axes[1].set_xlabel("Marketing Airline Network")
    axes[1].set_ylabel("Average Delay (minutes)")
    axes[1].set_title("Average Delay by Airline and Type")
    axes[1].legend()
    axes[1].grid(True)

    # Plot 3: Additional comprehensive delays comparison
    axes[2].plot(multiple_delay_causes['marketing_airline_network'], multiple_delay_causes['average_arrival_delay'], label='Average Arrival Delay', marker='o')
    axes[2].plot(multiple_delay_causes['marketing_airline_network'], multiple_delay_causes['average_departure_delay'], label='Average Departure Delay', marker='o')
    axes[2].set_xlabel("Marketing Airline Network")
    axes[2].set_ylabel("Average Delay (minutes)")
    axes[2].set_title("Comprehensive Delay Analysis by Airline")
    axes[2].legend()
    axes[2].grid(True)

    # Adjust layout to not cut off labels and display the plots
    plt.tight_layout()
    plt.show()

finally:
    session.shutdown()
    cluster.shutdown()
