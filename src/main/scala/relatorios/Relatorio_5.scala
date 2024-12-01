package relatorios

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, stddev, bround}
import plotly.Bar
import plotly.layout.{Axis, Layout}
import plotly.Plotly
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.element.Error.Data

object Relatorio_5 {

  def show(dfRenomeado: DataFrame): Unit = {
    // 5 - Média das temperaturas máximas e desvio padrão
    val dfMediaTemperaturaMaxima = dfRenomeado
      .agg(
        avg("temperatura_maxima").as("media_temperatura_maxima"),
        bround(stddev("temperatura_maxima"), 2).as("desvio_temperatura_maxima")  // Calculando o desvio padrão
      )

    dfMediaTemperaturaMaxima.show(false)

    // Coletar os valores da média e do desvio padrão
    val row = dfMediaTemperaturaMaxima.collect()(0)
    val mediaTemperaturaMaxima = row.getAs[Double]("media_temperatura_maxima")
    val desvioTemperaturaMaxima = row.getAs[Double]("desvio_temperatura_maxima")

    // Criar o gráfico de barras com erro no eixo Y (desvio padrão)
    val grafico = Bar(
      x = Seq("Média das Temperaturas Máximas"), // Nome da barra
      y = Seq(mediaTemperaturaMaxima) // Valor da média
    ).withName("Média da Temperatura Máxima")
      .withMarker(Marker().withColor(Color.RGBA(255, 99, 71, 0.6))) // Cor da barra
      .withError_y(
        Data(array = Seq(desvioTemperaturaMaxima)) // Barra de erro (desvio padrão)
          .withVisible(true) // Tornar a barra de erro visível
      )

    // Configurar o layout do gráfico
    val layout = Layout()
      .withTitle("Média das Temperaturas Máximas com Desvio Padrão")
      .withXaxis(Axis().withTitle("Tipo"))
      .withYaxis(Axis().withTitle("Temperatura Máxima (°C)"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "relatorios_grafico/5-media_temperatura_maxima.html"

    // Gerar e salvar o gráfico
    plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      openInBrowser = true // Abre automaticamente no navegador
    )
  }
}


/*
package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg

object Relatorio_5 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 5 - Média das temperaturas máximas
    val dfMediaTemperaturaMaxima = dfRenomeado
        .agg(avg("temperatura_maxima")
        .as("media_temperatura_maxima"))

    dfMediaTemperaturaMaxima.show(false)
  }
}
*/
