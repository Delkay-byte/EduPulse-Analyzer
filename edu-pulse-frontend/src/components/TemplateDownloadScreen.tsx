export default function TemplateDownloadScreen({ onUpload }: { onUpload: () => void }) {
  return (
    <div className="h-screen flex items-center justify-center bg-slate-950">
      <div className="bg-slate-900 p-8 rounded-2xl border border-slate-800 text-center max-w-md">
        <h2 className="text-xl font-black text-white mb-4">Download School Template</h2>
        <p className="text-sm text-slate-400 mb-8">
          Download the official CSV template, complete your school data, and upload it to sync with the district portal.
        </p>
        
        <div className="space-y-4">
          <button className="w-full bg-blue-600 hover:bg-blue-500 text-white font-bold py-3 rounded-lg transition">
            📥 Download Template
          </button>
          
          <div className="border-t border-slate-800 pt-4">
            <p className="text-xs text-slate-500 mb-4">Upload completed CSV file:</p>
            <button
              onClick={onUpload}
              className="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-3 rounded-lg transition"
            >
              📤 Upload Completed Data
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
