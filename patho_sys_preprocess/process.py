import os
import json
from typing import Dict, Any

from loguru import logger
import httpx
from pathlib import Path

from datamate.core.base_op import Mapper

import pandas as pd


# Custom logger format: Add 🟧🟧🟧 to all log startings and endings.
class OpsLogger:
    def __init__(self):
        self.logger = logger
        self.prefix = "🟧🟧🟧"
        self.appendix = "🟦🟦🟦"

    def debug(self, message: str):
        self.logger.debug(f"{self.prefix} {message} {self.appendix}")

    def info(self, message: str):
        self.logger.info(f"{self.prefix} {message} {self.appendix}")

    def warning(self, message: str):
        self.logger.warning(f"{self.prefix} {message} {self.appendix}")

    def error(self, message: str):
        self.logger.error(f"{self.prefix} {message} {self.appendix}")

ops_logger = OpsLogger()

class PathoSysPreprocess(Mapper):
    """
    病理系统数据预处理算子
    """

    def __init__(self, *args, **kwargs):
        """
        初始化参数
        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)

        # 路径的转换配置，支持如下三种模式：
        # 1. 保持不变："<>"（需要使用这个特殊符号的组合，而不是空字符串，因为空字符串会导致前端认为未填必填项）
        # 2. 源路径为相对路径，需要根据挂载点进行补全："<挂载点绝对路径>", 默认值为"/mnt/ruipath/hospital_data/"
        # 3. 源路径为绝对路径，或存在前缀替换需求的："<原前缀>:<新前缀>"，如 "storage/:/mnt/ruipath/hospital_data/"
        # 此配置项的默认配置为第二种模式，
        self.path_transformer = Path(kwargs.get('pathTransformer', '/mnt/ruipath/hospital_data/'))

        self.ignore_sdpc = kwargs.get('ignoreSdpc', False)

    def extra_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        额外的过滤条件
        :param df: 输入的 DataFrame
        :return: 过滤后的 DataFrame
        """
        # 示例：根据某列值进行过滤
        # df = df[df['some_column'] > threshold_value]
        return df

    def execute(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        """
        核心处理逻辑
        :param sample: 输入的数据样本，通常包含 text_key 等字段
        :return: 处理后的数据样本
        """

        file_path = sample.get('filePath', None)
        if not file_path:
            raise ValueError("Sample must contain valid 'filePath' key and value.")
        if not isinstance(file_path, str):
            raise TypeError("'filePath' must be a string.")
        if not file_path.endswith('.csv'):
            raise ValueError("'filePath' must point to a CSV file.")
        
        diagnosis_file_path = Path(file_path)
        diagnosis_file_name = diagnosis_file_path.name
        diagnosis_file_dir = diagnosis_file_path.parent

        ops_logger.info(f"Processing file: {diagnosis_file_path}")
        
        # >>> 读取包含 diagnosis 的 CSV 文件
        #     -----------------------------
        diagnosis_df = pd.read_csv(diagnosis_file_path)
        if not all(col in diagnosis_df.columns for col in ["case_no", "diagnosis"]):
            return sample

        # >>> 读取包含 slide_path 的 CSV 文件 
        #     ------------------------------
        try:
            slide_file_path = [f for f in os.listdir(diagnosis_file_dir) if f != diagnosis_file_name][0]
        except IndexError:
            ops_logger.error(f"No slide CSV file found in the directory.")
            return sample
        
        slide_info_df = pd.read_csv(os.path.join(diagnosis_file_dir, slide_file_path))
        if not all(col in slide_info_df.columns for col in ["case_no", "slide_path"]):
            return sample
        if not "thumbnail_path" in slide_info_df.columns:
            ops_logger.warning(f"No 'thumbnail_path' column found in slide CSV file. All SPDC files will be ignored.")
            self.ignore_sdpc = True

        ops_logger.info(f"File read: Diagnosis CSV: {diagnosis_df.shape}")
        ops_logger.info(f"File read: Slide CSV:     {slide_info_df.shape}")

        # >>> 合并 DataFrame 
        #     --------------

        merged_df = pd.merge(diagnosis_df, slide_info_df, on="case_no", how="inner")

        ops_logger.info(f"Data merged: {merged_df.shape}")

        # >>> 数据处理
        #     -------
        try:
            merged_df = self.data_processing(merged_df)
        except Exception as e:
            ops_logger.error(f"Data processing failed: {e}")
            return sample
        
        ops_logger.info(f"Data processed: {merged_df.shape}")

        # >>> 插入数据记录到数据集
        #     ------------------
        try:
            export_path = sample.get('export_path', None)
            if not export_path:
                ops_logger.error(f"Sample missing 'export_path' key or value.")
                raise ValueError("Sample must contain valid 'export_path' key and value.")

            merged_df = self.insert_into_dataset(merged_df, Path(export_path))

        except Exception as e:
            ops_logger.error(f"Failed to insert records into dataset: {e}")
            return sample
        
        ops_logger.info(f"Data inserted into dataset: {merged_df.shape}")

        # >>> 更新 sample
        #     -----------
        sample["text"] = merged_df.to_json(orient="records", force_ascii=False, indent=2)
        sample["fileName"] = f"case_diagnosis_slides.json"
        sample["fileType"] = "json"

        ops_logger.info(f"Sample updated with processed data.")

        return sample
    
    def data_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据处理逻辑
        :param df: 输入的 DataFrame
        :return: 处理后的 DataFrame
        """

        # >>> 处理非法值
        #     ---------

        # slide_path 为空值的行
        df = df[df["slide_path"].notna() & (df["slide_path"] != "")]

        # >>> 处理 SDPC 文件
        #     -------------
        if self.ignore_sdpc:
            df = df[~df["slide_path"].str.endswith('.sdpc')]
        else:
            df = df[~((df["slide_path"].str.endswith('.sdpc')) & 
                    (df["thumbnail_path"].isna() | (df["thumbnail_path"] == "")))]
            
        # >>> 处理文件路径转换
        #     ---------------
        df[["slide_path", "thumbnail_path"]] = df.apply(
            lambda row: (
                self.transform_path(row["slide_path"]),
                self.transform_path(row["thumbnail_path"]) if pd.notna(row["thumbnail_path"]) else ""
            ), axis=1, result_type='expand'
        )
            
        # >>> 其他过滤条件
        #     -----------
        df = self.extra_filters(df)

        return df
    
    def transform_path(self, known_path: str) -> str:
        """
        Adjusts a known path based on user input.
        - Empty string: returns original.
        - 'old:new': replaces prefix.
        - 'path': prepends as parent.
        """
        transform_rule = str(self.path_transformer)

        # 模式3：保持不变
        if not transform_rule.strip():
            return known_path

        known_p = Path(known_path)
        
        # 模式2：替换前缀（挂载点变更）
        if ":" in transform_rule:
            try:
                old_prefix, new_prefix = transform_rule.split(":", 1)
                # Convert to string to check start, then reconstruct
                path_str = str(known_p)
                if path_str.startswith(old_prefix):
                    # Replace the prefix and ensure it's a valid path again
                    updated_str = path_str.replace(old_prefix, new_prefix, 1)
                    return str(Path(updated_str))
                else:
                    ops_logger.warning(f"Warning: Prefix '{old_prefix}' not found in '{known_path}'")
                    return known_path
            except ValueError:
                ops_logger.error("Error: Invalid mapping format. Use 'old:new'")
                return known_path

        # 模式1：作为父目录添加为前缀
        new_parent = Path(transform_rule)
        return str(new_parent / known_p.relative_to(known_p.anchor) if known_p.is_absolute() else new_parent / known_p)
    
    def insert_into_dataset(self, merged_df: pd.DataFrame, export_path: Path) -> pd.DataFrame:
        """
        将合并后的数据插入到数据集中。注意，缩略图将依据 slide_path 进行重命名。
        :param merged_df: 合并后的 DataFrame
        :param export_path: 目标数据集路径，用于存放病理图片及其缩略图（可选）的软链接
        """

        # 插入数据集 - DataMate 内部 API 调用
        API_URL = f"http://datamate-backend:8080/api/data-management/datasets/{export_path.name}/files/upload/add"
        BATCH_SIZE = 1000

        ops_logger.info(f"DataMate connection API URL: {API_URL} | Batch Size: {BATCH_SIZE}")

        for start_idx in range(0, len(merged_df), BATCH_SIZE):
            end_idx = min(start_idx + BATCH_SIZE, len(merged_df))
            batch_df = merged_df.iloc[start_idx:end_idx]
            
            # 每行数据转换为字典：{"filePath": ..., "metadata": {...}}
            slide_records = []
            thumb_records = []
            for _, row in batch_df.iterrows():
                record = {"filePath": row["slide_path"]}
                metadata = row.drop(labels=["slide_path"]).to_dict()
                record["metadata"] = metadata

                slide_records.append(record)  # Slides File + Thumbnails Path + Metadata
                thumb_records.append({"filePath": row["thumbnail_path"]})  # Thumbnails File

            try:
                for t in ["slide", "thumbnail"]:
                    data = slide_records if t == "slide" else thumb_records
                    ops_logger.info(f"Uploading batch {start_idx}-{end_idx}: {len(data)} [[ {t} ]] records ")

                    request_body = {"files": data}
                    request_body_str = json.dumps(request_body, ensure_ascii=False)

                    curl_cmd = f'curl -X POST "{API_URL}" -H "Content-Type: application/json" -d \'{request_body_str}\' '
                    ops_logger.debug(f"Debug curl command for {t} records (batch {start_idx}-{end_idx}): {curl_cmd}")

                    response = httpx.post(API_URL, json={"files": data})
                    response.raise_for_status()

                    ops_logger.info(f"Successfully uploaded {t} records for batch {start_idx}-{end_idx}")

            except httpx.HTTPStatusError as e:
                response_body = e.response.text if e.response else "N/A"
                ops_logger.error(f"HTTP error uploading {t} records (batch {start_idx}-{end_idx}): {e}")
                ops_logger.error(f"Response body: {response_body}")
            except httpx.HTTPError as e:
                ops_logger.error(f"HTTP error uploading {t} records (batch {start_idx}-{end_idx}): {e}")
            except Exception as e:
                ops_logger.error(f"Failed to upload {t} records (batch {start_idx}-{end_idx}): {e}")

        return merged_df
