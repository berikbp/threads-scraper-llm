from transformers import pipeline

classifier = pipeline("text-classification", model="emotion_distilbert_model", return_all_scores=True)

print(classifier("I am very happy today!"))
print(classifier("I feel so sad and depressed."))
print(classifier("The weather is okay, nothing special."))
print(classifier("I am extremely joyful and excited about my new job!"))
print(classifier("For the first time in my life, I have never heard of a single one of these artists"))
