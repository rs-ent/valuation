import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def visualizer(distribution_result):
    df = pd.DataFrame(distribution_result)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # 인터랙티브한 시각화를 위해 Plotly 사용
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                        subplot_titles=('Metrics Timeline', 'Total Values'),
                        specs=[[{"type": "scatter"}],
                               [{"type": "scatter"}]])

    # 개별 가치 플롯
    components = ['FV_t', 'sv_t', 'apv_t', 'rv_t', 'cev_t', 'mcv_t', 'mds_t', 'mrv_t']
    colors = ['purple', 'green', 'blue', 'orange', 'red', 'brown', 'pink', 'gold']
    for i, component in enumerate(components):
        fig.add_trace(go.Scatter(x=df.index, y=df[component],
                                 mode='lines',
                                 name=component,
                                 line=dict(color=colors[i])),
                      row=1, col=1)

    # 총 가치 및 누적 가치 플롯
    df['cumulative_MOV_t'] = df['MOV_t'].cumsum()
    fig.add_trace(go.Scatter(x=df.index, y=df['MOV_t'],
                             mode='lines',
                             name='MOV_t',
                             line=dict(color='black', width=4)),
                  row=2, col=1)

    # 레이아웃 업데이트
    fig.update_layout(height=800, width=1200,
                      title_text="Artist Valuation Timeline",
                      showlegend=True)

    # X축 레이블 및 포맷 설정
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Value", row=1, col=1)
    fig.update_yaxes(title_text="Value", row=2, col=1)

    # 툴팁(hover) 포맷 지정
    fig.update_traces(hovertemplate='%{x}<br>%{y}')

    # 중요 이벤트 마커 추가 (예시)
    important_dates = [
        {'date': '2021-06-01', 'label': '신곡 발매'},
        {'date': '2022-01-15', 'label': '콘서트 개최'},
        {'date': '2022-09-10', 'label': '수상 경력'}
    ]

    for event in important_dates:
        date = pd.to_datetime(event['date'])
        if date in df.index:
            fig.add_vline(x=date, line_dash="dash", line_color="gray")
            fig.add_annotation(x=date, y=df['MOV_t'].max(), text=event['label'],
                               showarrow=True, arrowhead=1, ax=-40, ay=-40)

    # 그래프 출력
    fig.show()