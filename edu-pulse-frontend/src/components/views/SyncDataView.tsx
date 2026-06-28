import React, { useState } from 'react';
import { UploadCloud, FileSpreadsheet, CheckCircle, ArrowDownToLine } from 'lucide-react';

export const SyncDataView = () => {
  const [isUploading, setIsUploading] = useState(false);
  const [uploadStatus, setUploadStatus] = useState<'idle' | 'success' | 'error'>('idle');

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setIsUploading(true);
      // Simulate file parsing delay
      setTimeout(() => {
        setIsUploading(false);
        setUploadStatus('success');
      }, 2000);
    }
  };

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      
      {/* 1. DOWNLOAD TEMPLATE */}
      <div className="bg-slate-900 p-8 rounded-2xl border border-slate-800 flex items-center justify-between">
        <div>
          <h3 className="text-xl font-bold text-white">Step 1: Download Master Template</h3>
          <p className="text-slate-400 mt-1">Get the latest CSV template to input student records.</p>
        </div>
        <button className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white px-6 py-3 rounded-xl font-bold transition-all">
          <ArrowDownToLine size={20} /> Download CSV Template
        </button>
      </div>

      {/* 2. FILE UPLOAD */}
      <div className="bg-slate-900 p-8 rounded-2xl border border-slate-800">
        <h3 className="text-xl font-bold text-white mb-6">Step 2: Sync Student Data</h3>
        
        <div className={`border-2 border-dashed rounded-2xl p-12 text-center transition-all ${
          uploadStatus === 'success' ? 'border-emerald-500 bg-emerald-950/20' : 'border-slate-700 hover:border-blue-500'
        }`}>
          {uploadStatus === 'success' ? (
            <div className="flex flex-col items-center">
              <CheckCircle size={48} className="text-emerald-500 mb-4" />
              <h4 className="text-xl font-bold text-emerald-500">Upload Successful!</h4>
              <p className="text-slate-400 mt-2">Data synced with server. You can now view performance metrics.</p>
            </div>
          ) : (
            <label className="cursor-pointer flex flex-col items-center">
              <UploadCloud size={48} className="text-slate-500 mb-4" />
              <h4 className="text-lg font-bold text-white">Drop your CSV file here</h4>
              <p className="text-slate-400 text-sm mt-1">or click to browse from your computer</p>
              <input type="file" className="hidden" accept=".csv" onChange={handleFileUpload} />
            </label>
          )}
        </div>

        {/* Status Notification */}
        {isUploading && (
          <div className="mt-4 p-4 bg-slate-950 rounded-xl border border-slate-800 animate-pulse">
            <p className="text-blue-400 font-medium">Validating file structure... Checking headers...</p>
          </div>
        )}
      </div>

      {/* 3. SYNC LOGS (A lightweight version of the 'official_tab' feedback) */}
      <div className="bg-slate-900 p-6 rounded-2xl border border-slate-800">
        <h4 className="font-bold mb-4">Last Sync Attempt</h4>
        <div className="flex justify-between items-center bg-slate-950 p-4 rounded-xl border border-slate-800">
          <div className="flex items-center gap-3">
            <FileSpreadsheet className="text-slate-500" />
            <span className="text-sm font-mono text-slate-300">student_data_june_2026.csv</span>
          </div>
          <span className="text-xs font-bold text-emerald-500 bg-emerald-500/10 px-2 py-1 rounded">COMPLETED</span>
        </div>
      </div>
    </div>
  );
};
