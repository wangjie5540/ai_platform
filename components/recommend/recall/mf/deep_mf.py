import logging
import random

import numpy as np
import torch
from torch import nn
from torch.autograd import Variable
from torch.nn import functional as F
from torch.optim import Adam


class MfModel(nn.Module):

    def __init__(self):
        super(MfModel, self).__init__()
        self.user_emb = nn.Embedding(num_embeddings=90000, embedding_dim=32)
        self.item_emb = nn.Embedding(num_embeddings=90000, embedding_dim=32)
        self.droupout = nn.Dropout(p=0.5)
        self.dis_func = nn.CosineSimilarity()
        self.sigmoid_times = torch.from_numpy(np.array(6, dtype=np.float64))

    def forward(self, user_id, item_id):
        user_emb = self.user_emb(user_id)
        item_emb = self.item_emb(item_id)
        dis = self.dis_func(user_emb, item_emb)
        dis = torch.multiply(dis, self.sigmoid_times)
        return dis

    def cal_loss(self, dis, label, label_weight):
        return F.binary_cross_entropy_with_logits(dis, label, label_weight)


def train(input_file, num_epochs):
    mf_model = MfModel()
    optimizer = Adam(mf_model.parameters(), lr=0.01, weight_decay=0.000001)
    batch_size = 2048

    # Define your execution device
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    # Convert model parameters and buffers to CPU or Cuda
    mf_model.to(device)
    with open(input_file) as fi:
        lines = [_ for _ in fi]
    examples = []
    for _ in lines:
        vals = _.strip().split(",")
        if vals:
            user_id = int(vals[0])
            item_id = int(vals[1])
            label = int(vals[2])
            examples.append((user_id, item_id, label))
    logging.info(f"The model will be running on {device} device")
    for epoch in range(num_epochs):  # loop over the dataset multiple times
        random.shuffle(examples)
        running_loss = 0.0
        sample_cnt = 0
        for i in range(len(examples) // batch_size - 1):
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            batch_example = examples[start_idx:end_idx]
            user_ids = [_[0] for _ in batch_example]
            item_ids = [_[1] for _ in batch_example]
            labels = [_[2] for _ in batch_example]

            sample_cnt += len(user_ids)

            user_ids = torch.tensor(user_ids, dtype=torch.int64)
            item_ids = torch.tensor(item_ids, dtype=torch.int64)
            labels = torch.tensor(labels, dtype=torch.float)
            labels_weight = torch.tensor([1] * batch_size, dtype=torch.float)

            # get the inputs
            batch_user_id = Variable(user_ids.to(device))
            batch_item_id = Variable(item_ids.to(device))
            labels = Variable(labels.to(device))
            labels_weight = Variable(labels_weight.to(device))

            # zero the parameter gradients
            optimizer.zero_grad()
            # predict classes using images from the training set
            outputs = mf_model(batch_user_id, batch_item_id)
            # compute the loss based on model output and real labels
            loss = mf_model.cal_loss(outputs, labels, labels_weight)
            # backpropagate the loss
            loss.backward()
            # adjust parameters based on the calculated gradients
            optimizer.step()

            # Let's print statistics for every 1,000 images
            running_loss += loss.item()  # extract the loss value
            if i % 1000 == 1:
                # print every 1000 (twice per epoch)
                logging.info('[%d, %5d] loss: %.3fE-6' %
                             (epoch + 1, i + 1, running_loss / sample_cnt * 1E6))
                # zero the loss
                running_loss = 0.0
                sample_cnt = 0

        # # Compute and print the average accuracy fo this epoch when tested over all 10000 test images
        # accuracy = testAccuracy(mf_model, test_loader)
        # print('For epoch', epoch + 1, 'the test accuracy over the whole test set is %d %%' % (accuracy))

        # we want to save the model if the accuracy is the best
    return mf_model


def main():
    from common.logging_config import setup_console_log
    setup_console_log()
    input_file = "C:/Users/zhangxueren/Desktop/mf_train_dataset.csv"
    item_embeding_file = "C:/Users/zhangxueren/Desktop/recommend/emb/recall/item_embding.csv"
    user_embeding_file = "C:/Users/zhangxueren/Desktop/recommend/emb/recall/user_embeding.csv"
    import sys
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 3:
        item_embeding_file = sys.argv[2]
        user_embeding_file = sys.argv[3]
    mf_model = train(input_file, 3)
    with open(item_embeding_file, "w") as fo:
        item_emb = mf_model.item_emb.weight.detach().to('cpu').numpy().tolist()
        for i, item_vec in enumerate(item_emb):
            fo.write(f"{i},{item_vec}\n")
    with open(user_embeding_file, "w") as fo:
        user_emb = mf_model.user_emb.weight.detach().to('cpu').numpy().tolist()
        for i, user_vec in enumerate(user_emb):
            fo.write(f"{i},{user_vec}\n")
    logging.info(f"finish....")


if __name__ == '__main__':
    main()
