export const VerificationScreen = ({ onVerify }: { onVerify: () => void }) => {
  return (
    <div className="h-screen flex items-center justify-center bg-slate-950">
      <div className="bg-slate-900 p-8 rounded-2xl border border-slate-800 text-center max-w-sm">
        <h2 className="text-xl font-black text-white mb-6">Verify Circuit</h2>
        <input 
          type="text" 
          placeholder="Enter Circuit Code" 
          className="w-full bg-slate-950 p-3 rounded-lg text-white mb-4 border border-slate-700"
        />
        <button 
          onClick={onVerify}
          className="w-full bg-blue-600 text-white font-bold py-3 rounded-lg hover:bg-blue-500"
        >
          Verify School Circuit
        </button>
      </div>
    </div>
  );
};
