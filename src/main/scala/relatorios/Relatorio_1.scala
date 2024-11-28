package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col}
import plotly._
import plotly.layout._

object Relatorio_1 {
  def show(dfRenomeado: DataFrame): Unit = {
    // Calcular a temperatura média de todos os registros
    val resultado = dfRenomeado
      .withColumn("media_temperatura", (col("temperatura_maxima") + col("temperatura_minima")) / 2)
      .agg(avg("media_temperatura").as("temperatura_media_geral"))

    // Coletar o valor da média (será um único valor)
    val temperaturaMediaGeral = resultado.collect()(0).getAs[Double]("temperatura_media_geral")

    // Criar listas para o gráfico
    val temperaturaSeq = Seq(temperaturaMediaGeral)   // Eixo Y com o valor da temperatura média

    // Criar o gráfico de barras
    val grafico = Bar(
      x = Seq(""),  // Eixo X: Título único "Temperatura Média Geral"
      y = temperaturaSeq   // Eixo Y: Valor da temperatura média
    )

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Temperatura Média Geral")
      .withXaxis(
        Axis()
          .withTitle("Média")
          .withTickangle(-45)
          .withTickfont(Font().withSize(10))
      )
      .withYaxis(Axis().withTitle("Temperatura"))
      .withMargin(Margin(60, 30, 50, 100))

    // Gerar e salvar o gráfico
    val caminhoArquivo = "temperatura_media_geral.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      config = Config(),
      useCdn = true,
      openInBrowser = true,
      addSuffixIfExists = true
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
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
