import pandas as pd
from glob import glob

# Load and merge CSVs
files = glob("data/english/*.csv")

dfs = []
for f in files:
    try:
        temp = pd.read_csv(f)
        if not temp.empty:
            dfs.append(temp)
        else:
            print(f"⚠️ Skipping empty file: {f}")
    except Exception as e:
        print(f"⚠️ Could not read {f}: {e}")

df = pd.concat(dfs, ignore_index=True)
df.drop_duplicates(subset="id", inplace=True)
print("✅ Loaded", len(dfs), "files → total posts:", len(df))

# Clean text
df = df.dropna(subset=["text"])
df = df[df["text"].str.strip() != ""]

df["text"] = df["text"].astype(str)
df["text"] = df["text"].str.replace(r"http\S+", "", regex=True)
df["text"] = df["text"].str.replace(r"@\w+", "", regex=True)
df["text"] = df["text"].str.encode('ascii', 'ignore').str.decode('ascii')

# Add label column
df["label"] = df["emotion"].map({
    "sad": 0,
    "neutral": 1,
    "happy": 2
})

print(df["label"].value_counts())

# Save cleaned dataset
df.to_csv("data/english/cleaned_dataset.csv", index=False)
print("💾 Saved → data/english/cleaned_dataset.csv")

# Split into train/test
from sklearn.model_selection import train_test_split

train, test = train_test_split(df, test_size=0.2, stratify=df["label"], random_state=42)
train.to_csv("data/english/train.csv", index=False)
test.to_csv("data/english/test.csv", index=False)
print("✅ Train/Test split saved.")
