package relatorios

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count}
import plotly.Bar
import plotly.layout.{Axis, Layout}
import plotly.Plotly
import plotly.Plotly.plot
import plotly.element.{Color, Marker}

object Relatorio_4 {

  def show(dfRenomeado: DataFrame): Unit = {
    // 4 - Dias de alta umidade e alta temperatura (máxima > 30°C e umidade > 80%)
    val dfDiasAltaUmidadeETemperatura = dfRenomeado.filter(
      col("temperatura_maxima") > 30 &&
        col("umidade") > 80
    ).select(
      "data", "cidade", "temperatura_maxima", "umidade"
    )

    dfDiasAltaUmidadeETemperatura.show(dfRenomeado.count().toInt, truncate = false)

    // Agrupar por cidade e contar o número de dias que atendem a essa condição
    val dfDiasPorCidade = dfDiasAltaUmidadeETemperatura.groupBy("cidade")
      .agg(count("data").alias("dias_com_condicoes"))

    // Coletar os dados para o gráfico
    val dadosColetados = dfDiasPorCidade.collect().map(row =>
      (row.getAs[String]("cidade"), row.getAs[Long]("dias_com_condicoes"))
    )

    // Extrair os dados para os eixos X e Y
    val eixoX = dadosColetados.map(_._1) // Cidades no eixo X
    val eixoY = dadosColetados.map(_._2) // Número de dias no eixo Y

    // Criar o gráfico de barras
    val grafico = Bar(
      x = eixoX.toSeq,
      y = eixoY.toSeq
    ).withName("Dias com Alta Umidade e Alta Temperatura")
      .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.6))) // Cor diferenciada para o gráfico

    // Configurar o layout do gráfico
    val layout = Layout()
      .withTitle("Dias de Alta Umidade e Alta Temperatura (Máxima > 30°C, Umidade > 80%) por Cidade")
      .withXaxis(Axis().withTitle("Cidade").withTickangle(-45)) // Melhor visualização para nomes longos
      .withYaxis(Axis().withTitle("Número de Dias"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "relatorios_grafico/4-dias_alta_umidade_temperatura.html"

    // Gerar e salvar o gráfico
    plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      openInBrowser = true // Abre automaticamente no navegador
    )

    // Exibir os dados selecionados
    dfDiasPorCidade.show(false)
  }
}
