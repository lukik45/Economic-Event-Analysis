
# Economic Event Analysis

This project encompasses advanced natural language processing techniques to analyze economic events through fine-tuned language models and classify Federal Reserve speeches. The analysis utilizes state-of-the-art machine learning models tailored for financial contexts.

## Installation

Ensure you have Anaconda installed to manage the dependencies. Begin by cloning the repository and installing the required Python packages:

```bash
git clone https://github.com/lukik45/Economic-Event-Analysis.git
cd Economic-Event-Analysis
conda install --file requirements.txt
```

## Usage

### Step 1: Fine-tuning Masked Language Model (MLM)

1. **Vocabulary Scraping**:
   Download the A-Z vocabulary list and their definitions using `investopedia_scrapper.py`.

2. **Model Fine-tuning**:
   Fine-tune the pre-trained FinBERT model from Hugging Face with an additional financial dataset over 20 epochs. The training details are available in `mlm.ipynb`. Note that this process is resource-intensive and was performed on an NVIDIA A100 GPU.

### Step 2: Federal Reserve Speech Classification

1. **Data Preparation**:
   Speeches are downloaded using `fed_scrapper_until2010.py` and labeled based on VIX changes over time, sourced from Yahoo Finance. Due to the length of these speeches, they are split into manageable chunks using `langchain.text_splitter.RecursiveCharacterTextSplitter`.

2. **Model Inference**:
   Extract CLS token embeddings from the fine-tuned FinBERT model for each chunk of the speeches.

3. **Classification with LSTM**:
   Feed the embeddings to an LSTM network to classify the sentiment of the speech as either positive or negative based on the derived VIX labels.

## Results

After training the LSTM classifier for 10 epochs, the model achieved the following performance metrics:
- **Loss**: 0.1714
- **Accuracy**: 94.54%
- Detailed classification report is available in the LSTM training notebook.

### Testing and Evaluation

We recommend running the `testing.ipynb` for evaluating the model. This notebook contains all necessary testing components and configurations. The pre-trained model can be downloaded directly from Google Drive, with setup instructions provided within the notebook.

## Contributing

Contributions to this project are welcome. Please refer to the GitHub issues tab to discuss potential features or improvements.