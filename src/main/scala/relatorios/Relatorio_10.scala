package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly.Plotly.plot
import plotly._
import plotly.element.Error.Data
import plotly.element._
import plotly.layout._

object Relatorio_10 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 10 - Distribuição de umidade ao longo do ano (trimestres)
    val dfComData = dfRenomeado.withColumn("Data", to_date(col("data"), "yyyy-MM-dd"))

    // Adicionando coluna "Trimestre" baseado no mês
    val dfComTrimestre = dfComData.withColumn("Trimestre",
      when(month(col("Data")).between(1, 3), lit(1))
        .when(month(col("Data")).between(4, 6), lit(2))
        .when(month(col("Data")).between(7, 9), lit(3))
        .otherwise(lit(4))
    )

    // Agrupando por trimestre e calculando a umidade média e desvio padrão
    val dfUmidadePorTrimestre = dfComTrimestre.groupBy("Trimestre")
      .agg(
        avg("umidade").as("Umidade Média"),
        bround(stddev("umidade"), 2).as("Desvio Umidade") // Calculando o desvio padrão da umidade
      )
      .orderBy("Trimestre")

    // Exibindo a umidade média e desvio padrão por trimestre
    dfUmidadePorTrimestre.show(dfUmidadePorTrimestre.count().toInt, truncate = false)

    // Coletando os dados para o gráfico
    val dadosColetados = dfUmidadePorTrimestre.collect().map(row =>
      (
        row.getAs[Int]("Trimestre"),
        row.getAs[Double]("Umidade Média"),
        row.getAs[Double]("Desvio Umidade")
      )
    )

    val eixoX = dadosColetados.map(_._1) // Trimestres no eixo X
    val eixoY = dadosColetados.map(_._2) // Umidade média no eixo Y
    val erroY = dadosColetados.map(_._3) // Desvio padrão no eixo Y (erro)

    // Criar o gráfico de barras com erro no eixo Y (desvio padrão)
    val grafico = Bar(
      x = eixoX.toSeq,
      y = eixoY.toSeq
    ).withName("Umidade Média por Trimestre")
      .withMarker(Marker().withColor(Color.RGBA(30, 144, 255, 0.6))) // Cor diferenciada para o gráfico
      .withError_y(
        Data(array = erroY.toSeq) // Barra de erro (desvio padrão)
          .withVisible(true) // Tornar a barra de erro visível
      )

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Distribuição de Umidade ao Longo do Ano por Trimestres")
      .withWidth(800) // Largura do gráfico
      .withXaxis(Axis().withTitle("Trimestres"))
      .withYaxis(Axis().withTitle("Umidade Média (%)"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "umidade_por_trimestre_com_desvio.html"

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

object Relatorio_10 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 10 - Distribuição de umidade ao longo do ano (trimestres)
    val dfComData = dfRenomeado.withColumn("Data", to_date(col("data"), "yyyy-MM-dd"))

    // Adicionando coluna "Trimestre" baseado no mês
    val dfComTrimestre = dfComData.withColumn("Trimestre",
      when(month(col("Data")).between(1, 3), lit(1))
        .when(month(col("Data")).between(4, 6), lit(2))
        .when(month(col("Data")).between(7, 9), lit(3))
        .otherwise(lit(4))
    )

    // Agrupando por trimestre e calculando a umidade média
    val dfUmidadePorTrimestre = dfComTrimestre.groupBy("Trimestre")
      .agg(avg("umidade").as("Umidade Média"))
      .orderBy("Trimestre")

    // Exibindo a umidade média por trimestre
    dfUmidadePorTrimestre.show()
  }
}*/
