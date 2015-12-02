library(dplyr)
library(ggplot2)

# Compare batch vs stochastic VI (minibatch fraction 0.01) for 20 topics, kappa = 0.5

setwd("~/241a/project/svi-lda/results/results-batch-vi")
batch.nips <- read.csv("nips-20151129-203404.log", stringsAsFactors = FALSE)
batch.enron <- read.csv("enron-20151128-204027.log", stringsAsFactors = FALSE)
batch.nytimes <- read.csv("nytimes-20151128-214958.log", stringsAsFactors = FALSE)

setwd("~/241a/project/svi-lda/results/results-svi")
batch.clinton <- read.csv("iter-clinton-20-1.000-0.50-20151130-134024.log", stringsAsFactors = FALSE)
stoch.nips <- read.csv("iter-nips-20-0.010-0.50-20151129-210046.log", stringsAsFactors = FALSE)
stoch.enron <- read.csv("iter-enron-20-0.010-0.50-20151130-010326.log", stringsAsFactors = FALSE)
stoch.nytimes <- read.csv("iter-nytimes-20-0.010-0.50-20151130-023440.log", stringsAsFactors = FALSE)
stoch.clinton <- read.csv("iter-clinton-20-0.010-0.50-20151130-151205.log", stringsAsFactors = FALSE)

plotBatchVsStoch <- function(batchData, stochData, datasetName, maxTime) {
  ggplot() + 
    geom_line(data = batchData, aes(x = Time, y = LogLik, color = "Batch VI"), size = 1.5) +
    geom_line(data = stochData, aes(x = Time, y = LogLik, color = "SVI"), size = 1.5) +
    labs(x = "Time (seconds)", y = "Log likelihood", color = "Algorithm",
         title = paste("Batch vs stochastic VI for", datasetName, "corpus")) +
    xlim(0, maxTime)
}

plotBatchVsStoch(batch.nips, stoch.nips, "NIPS", 300)
ggsave("batch-vs-stoch-nips.png", width=6,height=4)
plotBatchVsStoch(batch.enron, stoch.enron, "Enron", 2600)
ggsave("batch-vs-stoch-enron.png", width=6,height=4)
plotBatchVsStoch(batch.nytimes, stoch.nytimes, "NYTimes", 50000)
ggsave("batch-vs-stoch-nytimes.png", width=6,height=4)
plotBatchVsStoch(batch.clinton, stoch.clinton, "Clinton", 2500)
ggsave("batch-vs-stoch-clinton.png", width=6,height=4)


# Varying minibatch size for 20 topics, kappa = 0.5

minibatchFractions <- c("0.500", "0.100", "0.010")
stoch.minibatch <- rbind_all(
  sapply(minibatchFractions, function(minibatchFraction) {
    minibatch.data <- read.csv(paste0("iter-nips-20-", minibatchFraction, 
                                      "-0.50-20151129-210046.log"))
    minibatch.data$MinibatchFraction <- minibatchFraction
    minibatch.data
  }, simplify = FALSE)
)

ggplot(data = stoch.minibatch) +
  geom_line(aes(x = Time, y = LogLik, color = MinibatchFraction), size = 1.5) +
  labs(x = "Time (seconds)", y = "Log likelihood", color = "Minibatch fraction", 
       title = paste("Varying minibatch fraction for NIPS corpus")) +
  xlim(0, 360)
ggsave("minibatch-stoch-nips.png", width=6,height=4)


# Varying kappa for minibatch fraction 0.01, 20 topics

kappas <- format(seq(0.6, 1.0, 0.1), nsmall=2)
stoch.kappa <- rbind_all(
  sapply(kappas, function(kappa) {
    kappa.data <- read.csv(paste0("iter-nips-20-0.010-", kappa, 
                                  "-20151129-213924.log"))
    kappa.data$Kappa <- kappa
    kappa.data
  }, simplify = FALSE)
)
stoch.nips$Kappa <- "0.50"
stoch.kappa <- rbind(stoch.kappa, stoch.nips)
stoch.nips <- stoch.nips %>% select(-Kappa)

ggplot(data = stoch.kappa) +
  geom_line(aes(x = Time, y = LogLik, color = Kappa), size = 1.5) +
  labs(x = "Time (seconds)", y = "Log likelihood", color = "Forgetting rate",
       title = paste("Varying forgetting rate for NIPS corpus")) +
  xlim(0, 360)
ggsave("kappa-stoch-nips.png", width=6,height=4)
# Looks like we didn't run SVI long enough for higher forgetting rates,
# since log likelihoods are still declining for most kappas. Oh well!

# Varying number of topics for minibatch fraction 0.01, kappa = 0.5
topicNumbers <- c(10, 50)
stoch.numTopics <- rbind_all(
  sapply(topicNumbers, function(numTopics) {
    numTopics.data <- read.csv(paste0("iter-nips-", numTopics, 
                                  "-0.010-0.50-20151129-224827.log"))
    numTopics.data$NumTopics <- numTopics
    numTopics.data
  }, simplify = FALSE)
)
stoch.nips$NumTopics <- "20"
stoch.numTopics <- rbind(stoch.numTopics, stoch.nips)

ggplot(data = stoch.numTopics) +
  geom_line(aes(x = Time, y = LogLik, color = NumTopics), size = 1.5) +
  labs(x = "Time (seconds)", y = "Log likelihood", color = "Number of topics",
       title = paste("Varying number of topics for NIPS corpus")) +
  xlim(0, 360)
ggsave("num-topics-stoch-nips.png", width=6,height=4)


# Get final topics learned by batch and stochastic VI
formatBatchTopics <- function(dataset, batchData, replace_zzz = FALSE) {
  finalTopics <- strsplit(last(batchData$Topics), "\\|")[[1]]
  formattedTopics <- lapply(
    finalTopics,
    function(wordsStr) {
      formatted <- gsub(" ", ",", wordsStr)
      if (replace_zzz) {
        # nytimes corpus represents proper nouns with zzz_ prefix
        formatted <- gsub("zzz_", "", formatted)
      }
      formatted
    }
  )
  writeLines(unlist(formattedTopics), con = paste0("batch-", dataset, "-topics.txt"))
}
formatBatchTopics("nips", batch.nips)
formatBatchTopics("enron", batch.enron)
formatBatchTopics("nytimes", batch.nytimes, replace_zzz = TRUE)
