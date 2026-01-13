import streamlit as st
import json
import pandas as pd
import plotly.express as px
from datetime import datetime
import os

def read_local_hdfs_data(base_path="/home/jovyan/work/hdfs-data"):
    """Lit les données depuis le système de fichiers local"""
    all_alerts = []
    
    try:
        # Parcourir tous les répertoires
        for country in os.listdir(base_path):
            country_path = os.path.join(base_path, country)
            if not os.path.isdir(country_path):
                continue
            
            for city in os.listdir(country_path):
                city_path = os.path.join(country_path, city)
                if not os.path.isdir(city_path):
                    continue
                
                # Lire alerts.json
                alert_file = os.path.join(city_path, "alerts.json")
                if os.path.exists(alert_file):
                    with open(alert_file, 'r') as f:
                        for line in f:
                            if line.strip():
                                try:
                                    alert = json.loads(line)
                                    all_alerts.append(alert)
                                except:
                                    pass
        
        return all_alerts
    except Exception as e:
        st.error(f"Erreur lecture données: {e}")
        return []

def parse_event_time(time_str):
    """Parse le timestamp de manière sécurisée"""
    try:
        if 'T' in time_str:
            time_str = time_str.replace('Z', '+00:00')
            return pd.to_datetime(time_str)
        else:
            return pd.to_datetime(time_str)
    except:
        return pd.NaT

def main():
    st.set_page_config(page_title="Weather Dashboard", layout="wide", page_icon="🌤️")
    
    st.title("🌤️ Dashboard Météo - Exercice 8")
    st.markdown("**Visualisation des données HDFS**")
    
    # Bouton de refresh
    if st.button("🔄 Rafraîchir les données"):
        st.rerun()
    
    # Chargement des données
    with st.spinner("Chargement des données..."):
        alerts = read_local_hdfs_data()
    
    if not alerts:
        st.error("❌ Aucune donnée trouvée.")
        st.info("Assurez-vous que l'exercice 7 est lancé et accumule des données.")
        st.code("python exo7_hdfs_via_namenode.py", language="bash")
        return
    
    # Conversion en DataFrame
    df = pd.DataFrame(alerts)
    df['event_time'] = df['event_time'].apply(parse_event_time)
    df = df.dropna(subset=['event_time'])
    df = df.sort_values('event_time')
    
    # Métriques globales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Alertes", len(df))
    
    with col2:
        st.metric("Villes", df['city'].nunique())
    
    with col3:
        avg_temp = df['temperature'].mean()
        st.metric("Température Moyenne", f"{avg_temp:.1f}°C")
    
    with col4:
        avg_wind = df['windspeed'].mean()
        st.metric("Vent Moyen", f"{avg_wind:.1f} m/s")
    
    st.markdown("---")
    
    # Filtres
    st.sidebar.header("🔍 Filtres")
    
    countries = ['Tous'] + sorted(df['country'].unique().tolist())
    selected_country = st.sidebar.selectbox("Pays", countries)
    
    if selected_country != 'Tous':
        cities = ['Toutes'] + sorted(df[df['country'] == selected_country]['city'].unique().tolist())
    else:
        cities = ['Toutes'] + sorted(df['city'].unique().tolist())
    selected_city = st.sidebar.selectbox("Ville", cities)
    
    # Appliquer les filtres
    df_filtered = df.copy()
    if selected_country != 'Tous':
        df_filtered = df_filtered[df_filtered['country'] == selected_country]
    if selected_city != 'Toutes':
        df_filtered = df_filtered[df_filtered['city'] == selected_city]
    
    # 1. Évolution température
    st.header("📈 1. Évolution de la Température")
    fig_temp = px.line(df_filtered, x='event_time', y='temperature', color='city',
                       title='Évolution de la température au fil du temps')
    fig_temp.update_layout(height=400)
    st.plotly_chart(fig_temp, use_container_width=True)
    
    # 2. Évolution vent
    st.header("💨 2. Évolution de la Vitesse du Vent")
    fig_wind = px.line(df_filtered, x='event_time', y='windspeed', color='city',
                       title='Évolution de la vitesse du vent')
    fig_wind.update_layout(height=400)
    st.plotly_chart(fig_wind, use_container_width=True)
    
    # 3. Alertes par niveau
    st.header("⚠️ 3. Distribution des Alertes par Niveau")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Alertes Vent")
        wind_alerts = df_filtered['wind_alert_level'].value_counts().reset_index()
        wind_alerts.columns = ['Niveau', 'Nombre']
        fig_wind_alerts = px.pie(wind_alerts, values='Nombre', names='Niveau',
                                 color='Niveau',
                                 color_discrete_map={
                                     'level_0': '#2ecc71',
                                     'level_1': '#f39c12',
                                     'level_2': '#e74c3c'
                                 })
        st.plotly_chart(fig_wind_alerts, use_container_width=True)
    
    with col2:
        st.subheader("Alertes Chaleur")
        heat_alerts = df_filtered['heat_alert_level'].value_counts().reset_index()
        heat_alerts.columns = ['Niveau', 'Nombre']
        fig_heat_alerts = px.pie(heat_alerts, values='Nombre', names='Niveau',
                                 color='Niveau',
                                 color_discrete_map={
                                     'level_0': '#2ecc71',
                                     'level_1': '#f39c12',
                                     'level_2': '#e74c3c'
                                 })
        st.plotly_chart(fig_heat_alerts, use_container_width=True)
    
    # 4. Code météo par pays
    st.header("🌍 4. Code Météo le Plus Fréquent par Pays")
    
    weather_code_map = {
        0: "Ciel dégagé", 1: "Dégagé", 2: "Nuageux", 3: "Couvert",
        45: "Brouillard", 61: "Pluie légère", 63: "Pluie modérée",
        65: "Pluie forte", 95: "Orage"
    }
    
    df_filtered['weather_description'] = df_filtered['weather_code'].map(
        lambda x: weather_code_map.get(x, f"Code {x}")
    )
    
    weather_by_country = df_filtered.groupby(['country', 'weather_description']).size().reset_index(name='count')
    top_weather = weather_by_country.sort_values(['country', 'count'], ascending=[True, False]).groupby('country').first().reset_index()
    
    fig_weather = px.bar(top_weather, x='country', y='count', color='weather_description',
                        title='Code Météo le Plus Fréquent par Pays')
    st.plotly_chart(fig_weather, use_container_width=True)
    
    # 5. Statistiques par ville
    st.header("📊 5. Statistiques Détaillées par Ville")
    
    city_stats = df_filtered.groupby(['country', 'city']).agg({
        'temperature': ['mean', 'min', 'max'],
        'windspeed': ['mean', 'max'],
        'wind_alert_level': lambda x: (x != 'level_0').sum(),
        'heat_alert_level': lambda x: (x != 'level_0').sum()
    }).round(2)
    
    city_stats.columns = ['Temp Moy (°C)', 'Temp Min (°C)', 'Temp Max (°C)', 
                          'Vent Moy (m/s)', 'Vent Max (m/s)',
                          'Alertes Vent', 'Alertes Chaleur']
    
    st.dataframe(city_stats, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(f"**Dernière mise à jour:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.markdown(f"**Nombre de données:** {len(df_filtered)} alertes")

if __name__ == "__main__":
    main()