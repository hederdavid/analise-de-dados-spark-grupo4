package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, bround, col, stddev}
import plotly.Plotly.plot
import plotly._
import plotly.element.Error.Data
import plotly.layout._

object Relatorio_1 extends App {
  def show(dfRenomeado: DataFrame): Unit = {
    // Calcular a temperatura média de todos os registros
    val resultado = dfRenomeado
      .withColumn("media_temperatura", (col("temperatura_maxima") + col("temperatura_minima")) / 2)
      .agg(
        avg("media_temperatura").as("temperatura_media_geral"),
        bround(stddev("media_temperatura"), 2).as("desvio_padrao_temperatura")  // Calculando o desvio padrão e arredondando
      )

    resultado.show(false)

    // Coletar os valores da média e desvio padrão (serão valores únicos)
    val row = resultado.collect()(0)
    val temperaturaMediaGeral = row.getAs[Double]("temperatura_media_geral")
    val desvioPadraoTemperatura = row.getAs[Double]("desvio_padrao_temperatura")

    // Criar listas para o gráfico
    val temperaturaSeq = Seq(temperaturaMediaGeral)   // Eixo Y com o valor da temperatura média
    val desvioSeq = Seq(desvioPadraoTemperatura)      // Eixo Y para o desvio padrão

    // Criar o gráfico de barras com erro no eixo Y (desvio padrão)
    val graficoTemperatura = Bar(
      x = Seq("Temperatura Média"),
      y = temperaturaSeq,  // Eixo Y: Valor da temperatura média
    ).withName("Temperatura Média")
      .withError_y(
        Data(array = desvioSeq)  // Adicionando o erro com o desvio padrão
          .withVisible(true)  // Tornando o erro visível
      )

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Temperatura Média Geral e Desvio Padrão")
      .withXaxis(Axis().withTitle("Média e Desvio"))
      .withYaxis(Axis().withTitle("Temperatura (°C)"))
      .withMargin(Margin(60, 30, 50, 100))

    // Gerar e salvar o gráfico
    val caminhoArquivo = "relatorios_grafico/1-temperatura_media_geral.html"
    plot(
      path = caminhoArquivo,
      traces = Seq(graficoTemperatura),
      layout = layout,
      openInBrowser = true,
    )
  }
}

/*package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col}

object Relatorio_1 {
  def show (dfRenomeado: DataFrame): Unit = {
    // Calcule a temperatura média (média entre a máxima e a mínima) de todos os registros.
    val resultado = dfRenomeado
      .withColumn("media_temperatura", (col("temperatura_maxima") + col("temperatura_minima")) / 2)
      .agg(avg("media_temperatura").as("temperatura_media_geral"))

    // Exibindo o resultado completo (sem truncamento)
    resultado.show(false)
  }
}*/
