# 기본 이미지 선택. 여기서는 Node.js 공식 이미지 사용
FROM node:18

# 작업 디렉토리 설정
WORKDIR /usr/src/app

# 의존성 파일 복사 (package.json 등)
COPY package*.json ./

# 애플리케이션 의존성 설치
RUN npm install

# 애플리케이션 소스 복사
COPY . .

# 애플리케이션 실행을 위한 포트 설정
EXPOSE 9090

# 애플리케이션 시작 명령어
CMD ["node", "index.js"]
