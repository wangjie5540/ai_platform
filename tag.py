
with open('aaa.txt') as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        image, version = line.split(' ')
        tag = image.split('/')[-1]
        tag_cmd = f'docker tag {image}:{version} digit-force-docker.pkg.coding.net/ai-platform/third/{tag}:{version}'
        print(tag_cmd)