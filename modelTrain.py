import pandas as pd
import torch 
from torch.utils.data import DataLoader, Dataset
from sklearn.model_selection import train_test_split
from transformers import (
    DistilBertTokenizerFast,
    DistilBertForSequenceClassification,
    Trainer,
    TrainingArguments,
) 

train_df = pd.read_csv("data/english/train.csv")
test_df = pd.read_csv("data/english/test.csv")

#INSTALLATE TOKENIZER
tokenizer = DistilBertTokenizerFast.from_pretrained('distilbert-base-uncased')

train_encodings = tokenizer(
    train_df['text'].tolist(),
    truncation=True,
    padding=True,
    max_length=128
)
test_encodings = tokenizer(
    test_df['text'].tolist(),
    truncation=True,
    padding=True,
    max_length=128
)

class EmotionDataset(Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels
    
    def __getitem__(self, index):
        item = {key: torch.tensor(val[index]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[index])
        return item
    
    def __len__(self):
        return len(self.labels)

train_dataset = EmotionDataset(train_encodings, train_df['label'].tolist())
test_dataset = EmotionDataset(test_encodings, test_df['label'].tolist())

model = DistilBertForSequenceClassification.from_pretrained(
    'distilbert-base-uncased',
    num_labels=3
)

training_args = TrainingArguments(
    output_dir="./results",
    do_eval=True,                    # replaces evaluation_strategy
    num_train_epochs=2,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    learning_rate=5e-5,
    weight_decay=0.01,
    logging_dir="./logs",
)


#Train the model
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=test_dataset,
)
trainer.train()

metrics = trainer.evaluate()
print("Evaluation metrics:", metrics)

#Save the model
save_path = "emotion_distilbert_model"
model.save_pretrained(save_path)
tokenizer.save_pretrained(save_path)
print(f"Model and tokenizer saved to {save_path}")

from transformers import pipeline
classifier = pipeline("text-classification", model=save_path, tokenizer=save_path)
print(classifier("I am very happy today!"))
print(classifier("I feel so sad and depressed."))
print(classifier("The weather is okay, nothing special."))

