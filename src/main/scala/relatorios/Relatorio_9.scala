package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly.Plotly.plot
import plotly._
import plotly.element._
import plotly.layout._

object Relatorio_9 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 9 - Diferença entre temperaturas máxima e mínima: Maiores variações por cidade
    val dfComDiferenca = dfRenomeado.withColumn(
      "Diferenca Temperatura",
      col("temperatura_maxima") - col("temperatura_minima")
    )

    dfComDiferenca.show(dfComDiferenca.count().toInt, truncate = false)

    // Agrupar por cidade e calcular a maior variação de temperatura
    val dfVariacaoPorCidade = dfComDiferenca.groupBy("cidade")
      .agg(max("Diferenca Temperatura").as("Maior Variação de Temperatura"))
      .orderBy(desc("Maior Variação de Temperatura"))

    // Exibindo as maiores variações de temperatura por cidade


    // Coletando os dados para o gráfico
    val dadosColetados = dfVariacaoPorCidade.collect().map(row =>
      (
        row.getAs[String]("cidade"),
        row.getAs[Double]("Maior Variação de Temperatura")
      )
    )

    val eixoX = dadosColetados.map(_._1) // Cidades no eixo X
    val eixoY = dadosColetados.map(_._2) // Maior variação de temperatura no eixo Y

    // Criar o gráfico de barras
    val grafico = Bar(
      x = eixoX.toSeq,
      y = eixoY.toSeq
    ).withName("Maior Variação de Temperatura por Cidade")
      .withMarker(Marker().withColor(Color.RGBA(255, 69, 0, 0.6))) // Cor diferenciada para o gráfico

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Maior Variação de Temperatura por Cidade")
      .withWidth(800) // Largura do gráfico
      .withXaxis(Axis().withTitle("Cidade"))
      .withYaxis(Axis().withTitle("Maior Variação de Temperatura (°C)"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "maior_variacao_temperatura_por_cidade.html"

    // Gerar e salvar o gráfico
    plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      openInBrowser = true // Abre automaticamente no navegador
    )
  }
}


/*package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Relatorio_9 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 9 - Diferença entre temperaturas máxima e mínima: Maiores variações por cidade
    val dfComDiferenca = dfRenomeado.withColumn(
      "Diferenca Temperatura",
      col("temperatura_maxima") - col("temperatura_minima")
    )

    val dfVariacaoPorCidade = dfComDiferenca.groupBy("cidade")
      .agg(max("Diferenca Temperatura").as("Maior Variação de Temperatura"))
      .orderBy(desc("Maior Variação de Temperatura"))

    // Exibindo as maiores variações de temperatura por cidade
    dfVariacaoPorCidade.show()
  }
}*/
