package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}
import plotly.Bar
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.layout.{Axis, Layout}

object Relatorio_2 {

  def show(dfRenomeado: DataFrame): Unit = {

    // Adicionar a coluna de diferença de temperatura
    val dfVariacaoTemperatura = dfRenomeado.withColumn(
      "diferenca_temperatura",
      col("temperatura_maxima") - col("temperatura_minima")
    )

    // Filtrar os dias com grande variação de temperatura (> 15°C)

    val dfDiasComGrandeVariacao = dfVariacaoTemperatura.filter(
      col("diferenca_temperatura") > 20
    ).select(
      "data", "cidade", "temperatura_maxima", "temperatura_minima"
    )

    dfDiasComGrandeVariacao.show(dfRenomeado.count().toInt, truncate = false)

    // Contar a frequência de cada cidade
    val dfContagemCidades = dfDiasComGrandeVariacao
      .groupBy("cidade")
      .agg(count("cidade").alias("quantidade"))
      .select("cidade", "quantidade")

    // Coletar os dados para o gráfico
    val dadosColetados = dfContagemCidades.collect().map(row =>
      (row.getAs[String]("cidade"), row.getAs[Long]("quantidade"))
    )

    val eixoX = dadosColetados.map(_._1) // Nomes das cidades
    val eixoY = dadosColetados.map(_._2) // Quantidade de vezes que aparecem

    // Criar o gráfico de barras
    val grafico = Bar(
      x = eixoX.toSeq,
      y = eixoY.toSeq
    ).withName("Frequência de Cidades")
      .withMarker(Marker().withColor(Color.RGB(235, 134, 196))) // Cor diferenciada para o gráfico

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Cidades com Grande Variação de Temperatura (> 20°C)")
      .withWidth(800) // Aumente bastante a largura para criar o efeito de rolagem
      .withXaxis(Axis().withTitle("Cidade").withTickangle(-40))
      .withYaxis(
        Axis()
          .withTitle("Quantidade de Ocorrências")
      )

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "relatorios_grafico/2-frequencia_cidades.html"

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
import org.apache.spark.sql.functions.col

object Relatorio_2 {
  def show(dfRenomeado: DataFrame): Unit = {
    // Calcular a diferença de temperatura entre a máxima e a mínima
    val dfVariacaoTemperatura = dfRenomeado.withColumn(
      "diferenca_temperatura",
      col("temperatura_maxima") - col("temperatura_minima")
    )

    // Filtrando os dias com grande variação de temperatura (> 15°C)
    val dfDiasComGrandeVariacao = dfVariacaoTemperatura.filter(
      col("diferenca_temperatura") > 15
    ).select(
      "data", "cidade", "temperatura_maxima", "temperatura_minima"
    )

    // Exibindo o resultado completo (sem truncamento)
    dfDiasComGrandeVariacao.show(dfRenomeado.count().toInt, truncate = false)
  }
}*/

